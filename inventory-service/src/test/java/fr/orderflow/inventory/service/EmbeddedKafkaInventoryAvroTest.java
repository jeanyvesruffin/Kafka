package fr.orderflow.inventory.service;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.test.KafkaTestSupport;
import fr.orderflow.common.test.OrderCreatedMessages;
import fr.orderflow.inventory.domain.StockEntity;
import fr.orderflow.inventory.repository.StockRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Phase 7 : {@code orders.created} en Avro, vu depuis inventory-service, sur un broker embarque.
 *
 * <p>Evolution de schema en conditions reelles : le contrat est passe de la version 1 a la version 2
 * (champ optionnel {@code couponCode}). Pendant un deploiement, des producteurs des deux versions
 * coexistent ; le consommateur a jour doit lire les deux.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {
        Topics.ORDERS_CREATED, Topics.ORDERS_CANCELLED,
        Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED})
@DirtiesContext
class EmbeddedKafkaInventoryAvroTest {

    private static final String CID = "corr-avro";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private StockRepository stockRepository;
    @Autowired
    private EventSerializer eventSerializer;
    @MockitoSpyBean
    private InventoryService inventoryService;

    private KafkaTestSupport kafka;
    private String productId;

    @BeforeEach
    void setUp() {
        kafka = new KafkaTestSupport(brokers);
        productId = "sku-" + UUID.randomUUID();
        stockRepository.save(new StockEntity(productId, 10));
    }

    private OrderCreatedEvent orderCreated(int quantity, String couponCode) {
        OrderLine line = new OrderLine(productId, quantity, new BigDecimal("19.90"));
        return new OrderCreatedEvent(
                UUID.randomUUID().toString(), "ord-" + UUID.randomUUID(), "cust-118",
                List.of(line), line.lineTotal(), Instant.now(), couponCode);
    }

    private StockEntity stock() {
        return stockRepository.findById(productId).orElseThrow();
    }

    @Test
    @DisplayName("Producteur de la version COURANTE (avec coupon) : stock reserve, InventoryReserved publie")
    void currentVersionMessage_isProcessed() {
        OrderCreatedEvent event = orderCreated(3, "PROMO10");

        OrderCreatedMessages.publish(kafka, event, CID);

        ConsumerRecord<String, byte[]> reserved = kafka.awaitRecord(Topics.INVENTORY_RESERVED, event.orderId());
        assertThat(KafkaTestSupport.header(reserved, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        await().atMost(KafkaTestSupport.TIMEOUT).untilAsserted(() -> {
            assertThat(stock().getQuantityAvailable()).isEqualTo(7);
            assertThat(stock().getQuantityReserved()).isEqualTo(3);
        });
    }

    @Test
    @DisplayName("Producteur de la version 1 PAS ENCORE REDEPLOYE (sans coupon) : le consommateur a jour le lit toujours")
    void version1Message_isStillProcessedByTheUpToDateConsumer() {
        OrderCreatedEvent event = orderCreated(2, null);

        OrderCreatedMessages.publishAsVersion1(kafka, event, CID);

        kafka.awaitRecord(Topics.INVENTORY_RESERVED, event.orderId());
        await().atMost(KafkaTestSupport.TIMEOUT).untilAsserted(() -> {
            assertThat(stock().getQuantityAvailable()).isEqualTo(8);
            assertThat(stock().getQuantityReserved()).isEqualTo(2);
        });
        assertThat(kafka.records(Topics.dltOf(Topics.ORDERS_CREATED), event.orderId())).isEmpty();
    }

    @Test
    @DisplayName("Ancien format JSON sur orders.created (messages d'avant la migration) : DLT immediat, rien n'est reserve")
    void legacyJsonMessage_goesToDeadLetterTopic() {
        OrderCreatedEvent event = orderCreated(2, null);
        String legacyJson = eventSerializer.toJson(event);

        kafka.send(Topics.ORDERS_CREATED, event.orderId(), legacyJson,
                Map.of(EventHeaders.CORRELATION_ID, CID, EventHeaders.CONTENT_TYPE, "application/json"));

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(Topics.dltOf(Topics.ORDERS_CREATED), event.orderId());
        assertThat(KafkaTestSupport.text(dead)).isEqualTo(legacyJson);   // rejouable apres conversion
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .as(KafkaTestSupport.describeHeaders(dead))
                .contains("DeserializationException");
        verify(inventoryService, never()).handleOrderCreated(any(), any());
        assertThat(stock().getQuantityAvailable()).isEqualTo(10);
        assertThat(stock().getQuantityReserved()).isZero();
    }
}
