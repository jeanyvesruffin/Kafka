package fr.orderflow.inventory.service;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.messaging.avro.OrderCreatedAvroCodec;
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
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Phase 4 : reprises <b>bloquantes</b> et Dead Letter Topic d'inventory-service.
 *
 * <p>{@link InventoryService} est espionne : le test lui fait lever une
 * {@code OptimisticLockingFailureException} (le conflit de version que provoquent deux commandes
 * concurrentes sur le meme produit) a la place de la vraie. Les delais de reprise sont raccourcis
 * par {@code src/test/resources/application.yml} (50 ms, 3 reprises).
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {
        Topics.ORDERS_CREATED, Topics.ORDERS_CANCELLED,
        Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED})
@DirtiesContext
class EmbeddedKafkaInventoryRetryTest {

    private static final String CID = "corr-retry";
    private static final String DLT = Topics.dltOf(Topics.ORDERS_CREATED);

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private StockRepository stockRepository;
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

    @Test
    @DisplayName("Conflit de verrou optimiste : le message est rejoue sur place, la reservation finit par aboutir")
    void optimisticLockingFailure_isRetried_thenSucceeds() {
        OrderCreatedEvent event = orderCreated(3);
        ObjectOptimisticLockingFailureException conflict =
                new ObjectOptimisticLockingFailureException(StockEntity.class, productId);
        // Deux echecs, puis le vrai traitement : ce que donnerait un conflit passager.
        doThrow(conflict).doThrow(conflict).doCallRealMethod()
                .when(inventoryService)
                .handleOrderCreated(argThat(e -> e != null && e.orderId().equals(event.orderId())), anyString());

        publish(event);

        kafka.awaitRecord(Topics.INVENTORY_RESERVED, event.orderId());
        verify(inventoryService, times(3))
                .handleOrderCreated(argThat(e -> e != null && e.orderId().equals(event.orderId())), anyString());
        assertThat(kafka.records(DLT, event.orderId())).isEmpty();
        assertThat(kafka.records(Topics.INVENTORY_RESERVED, event.orderId())).hasSize(1);
        assertThat(stock().getQuantityAvailable()).isEqualTo(7);
        assertThat(stock().getQuantityReserved()).isEqualTo(3);
    }

    @Test
    @DisplayName("Verrou jamais libere : apres les reprises le message part en DLT, le stock est intact et la partition repart")
    void persistentOptimisticLockingFailure_endsInDeadLetterTopic_withoutBlockingTheNextOrder() throws IOException {
        OrderCreatedEvent stuck = orderCreated(3);
        doThrow(new ObjectOptimisticLockingFailureException(StockEntity.class, productId))
                .when(inventoryService)
                .handleOrderCreated(argThat(e -> e != null && e.orderId().equals(stuck.orderId())), anyString());

        publish(stuck);

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(DLT, stuck.orderId());
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN))
                .contains("OptimisticLockingFailureException");
        assertThat(KafkaTestSupport.header(dead, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        // Le message avait ete deserialise avant d'echouer : il est reserialise en Avro pour le DLT
        assertThat(new OrderCreatedAvroCodec().decode(dead.value()).orderId()).isEqualTo(stuck.orderId());
        // 1 tentative + 3 reprises (orderflow.kafka.retry.blocking.max-retries du profil de test)
        verify(inventoryService, times(4))
                .handleOrderCreated(argThat(e -> e != null && e.orderId().equals(stuck.orderId())), anyString());
        assertThat(kafka.records(Topics.INVENTORY_RESERVED, stuck.orderId())).isEmpty();
        assertThat(stock().getQuantityAvailable()).isEqualTo(10);
        assertThat(stock().getQuantityReserved()).isZero();

        // Le message en DLT ne retient rien : la commande suivante est traitee normalement.
        OrderCreatedEvent next = orderCreated(2);
        publish(next);
        kafka.awaitRecord(Topics.INVENTORY_RESERVED, next.orderId());
        await().atMost(KafkaTestSupport.TIMEOUT)
                .untilAsserted(() -> assertThat(stock().getQuantityReserved()).isEqualTo(2));
    }

    @Test
    @DisplayName("Octets illisibles sur orders.created (Avro) : DeserializationException, DLT immediat avec les octets d'origine")
    void undecodableAvro_goesStraightToDeadLetterTopic() {
        String orderId = "ord-poison-" + UUID.randomUUID();
        byte[] garbage = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12};

        kafka.send(Topics.ORDERS_CREATED, orderId, garbage, Map.of(EventHeaders.CORRELATION_ID, CID));

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(DLT, orderId);
        assertThat(dead.value()).isEqualTo(garbage);   // octets d'origine, rejouables apres correction
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_FQCN))
                .as(KafkaTestSupport.describeHeaders(dead))
                .contains("DeserializationException");
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(Topics.ORDERS_CREATED);
        verify(inventoryService, never()).handleOrderCreated(any(), any());

        // Le message empoisonne ne retient rien : une commande valide passe juste apres.
        OrderCreatedEvent next = orderCreated(1);
        publish(next);
        kafka.awaitRecord(Topics.INVENTORY_RESERVED, next.orderId());
    }

    @Test
    @DisplayName("JSON illisible sur orders.cancelled (topic JSON) : DLT immediat, le service n'est jamais appele")
    void malformedJson_goesStraightToDeadLetterTopic() {
        String orderId = "ord-poison-" + UUID.randomUUID();
        String garbage = "{\"eventId\": \"abc\", ceci n'est pas du JSON";

        kafka.send(Topics.ORDERS_CANCELLED, orderId, garbage, Map.of(EventHeaders.CORRELATION_ID, CID));

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(Topics.dltOf(Topics.ORDERS_CANCELLED), orderId);
        assertThat(KafkaTestSupport.text(dead)).isEqualTo(garbage);   // octets d'origine, rejouables
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).contains("jackson");
        verify(inventoryService, never()).handleOrderCancelled(any());
    }

    // ------------------------------------------------------------------

    private OrderCreatedEvent orderCreated(int quantity) {
        OrderLine line = new OrderLine(productId, quantity, new BigDecimal("19.90"));
        return new OrderCreatedEvent(
                UUID.randomUUID().toString(), "ord-" + UUID.randomUUID(), "cust-118",
                List.of(line), line.lineTotal(), Instant.now());
    }

    private StockEntity stock() {
        return stockRepository.findById(productId).orElseThrow();
    }

    private void publish(OrderCreatedEvent event) {
        OrderCreatedMessages.publish(kafka, event, CID);   // orders.created est en Avro (phase 7)
    }
}
