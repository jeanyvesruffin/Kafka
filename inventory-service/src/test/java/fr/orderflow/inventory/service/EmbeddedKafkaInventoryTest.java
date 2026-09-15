package fr.orderflow.inventory.service;

import fr.orderflow.common.event.*;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.inventory.domain.StockEntity;
import fr.orderflow.inventory.repository.StockRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tests d'integration Kafka du service Inventory, sur un broker embarque.
 *
 * <p>Le test joue le role de order-service : il publie sur {@code orders.*}
 * exactement ce que l'outbox publierait (payload JSON + en-tetes), puis
 * observe ce que Inventory publie en retour et l'etat du stock en base.
 *
 * <p>Chaque test cree son propre produit : les tests partagent le broker et
 * la base mais ne se marchent pas dessus.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {
        Topics.ORDERS_CREATED, Topics.ORDERS_CANCELLED,
        Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED})
@DirtiesContext
class EmbeddedKafkaInventoryTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String CID = "corr-embedded";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private EventSerializer eventSerializer;
    @Autowired
    private StockRepository stockRepository;

    private String productId;

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers()
                .lastHeader(name)
                .value(), StandardCharsets.UTF_8);
    }

    @BeforeEach
    void createProduct() {
        productId = "sku-" + UUID.randomUUID();
        stockRepository.save(new StockEntity(productId, 10));
    }

    @Test
    @DisplayName("OrderCreated avec stock suffisant : stock reserve et InventoryReserved publie (cle et correlationId conserves)")
    void orderCreated_reservesStockAndPublishesInventoryReserved() {
        OrderCreatedEvent event = orderCreated(3);

        publish(Topics.ORDERS_CREATED, event);

        ConsumerRecord<String, String> record = awaitRecord(Topics.INVENTORY_RESERVED, event.orderId());
        assertThat(header(record, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        assertThat(header(record, EventHeaders.EVENT_TYPE)).isEqualTo(InventoryReservedEvent.TYPE);
        InventoryReservedEvent reserved = eventSerializer.fromJson(record.value(), InventoryReservedEvent.class);
        assertThat(reserved.customerId()).isEqualTo("cust-118");
        assertThat(reserved.items()).extracting(OrderLine::productId)
                .containsExactly(productId);

        await().atMost(TIMEOUT)
                .untilAsserted(() -> {
                    assertThat(stock().getQuantityAvailable()).isEqualTo(7);
                    assertThat(stock().getQuantityReserved()).isEqualTo(3);
                });
    }

    @Test
    @DisplayName("OrderCreated avec stock insuffisant : rien n'est reserve et InventoryRejected est publie")
    void orderCreated_withoutEnoughStock_publishesInventoryRejected() {
        OrderCreatedEvent event = orderCreated(11);

        publish(Topics.ORDERS_CREATED, event);

        ConsumerRecord<String, String> record = awaitRecord(Topics.INVENTORY_REJECTED, event.orderId());
        assertThat(header(record, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        InventoryRejectedEvent rejected = eventSerializer.fromJson(record.value(), InventoryRejectedEvent.class);
        assertThat(rejected.reason()).contains(productId);

        assertThat(stock().getQuantityAvailable()).isEqualTo(10);
        assertThat(stock().getQuantityReserved()).isZero();
    }

    @Test
    @DisplayName("Un OrderCreated redelivre (meme eventId) ne reserve le stock qu'une fois (idempotence)")
    void redeliveredOrderCreated_reservesOnlyOnce() {
        OrderCreatedEvent event = orderCreated(3);

        publish(Topics.ORDERS_CREATED, event);
        publish(Topics.ORDERS_CREATED, event);   // redelivery apres rebalance

        awaitRecord(Topics.INVENTORY_RESERVED, event.orderId());
        // Meme cle => meme partition : le doublon est consomme juste apres
        // l'original. Le stock doit rester stable pendant qu'il passe.
        await().during(Duration.ofSeconds(3))
                .atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(stock().getQuantityAvailable()).isEqualTo(7));
        assertThat(recordsFor(Topics.INVENTORY_RESERVED, event.orderId())).hasSize(1);
    }

    @Test
    @DisplayName("OrderCancelled libere le stock reserve (compensation)")
    void orderCancelled_releasesReservedStock() {
        OrderCreatedEvent created = orderCreated(3);
        publish(Topics.ORDERS_CREATED, created);
        // Deux topics distincts : aucun ordre garanti entre eux, on attend la reservation.
        await().atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(stock().getQuantityReserved()).isEqualTo(3));

        publish(Topics.ORDERS_CANCELLED, new OrderCancelledEvent(
                UUID.randomUUID()
                        .toString(), created.orderId(), created.items(), "Paiement refuse", Instant.now()));

        await().atMost(TIMEOUT)
                .untilAsserted(() -> {
                    assertThat(stock().getQuantityAvailable()).isEqualTo(10);
                    assertThat(stock().getQuantityReserved()).isZero();
                });
    }

    // ------------------------------------------------------------------

    private OrderCreatedEvent orderCreated(int quantity) {
        OrderLine line = new OrderLine(productId, quantity, new BigDecimal("19.90"));
        return new OrderCreatedEvent(
                UUID.randomUUID()
                        .toString(), "ord-" + UUID.randomUUID(), "cust-118",
                List.of(line), line.lineTotal(), Instant.now());
    }

    private StockEntity stock() {
        return stockRepository.findById(productId)
                .orElseThrow();
    }

    /**
     * Publie l'evenement tel que le ferait order-service : cle = orderId, en-tetes standards.
     */
    private void publish(String topic, OrderFlowEvent event) {
        EventEnvelope envelope = eventSerializer.envelope(topic, event, CID);
        var record = new ProducerRecord<>(envelope.topic(), envelope.key(), envelope.payload());
        envelope.headers()
                .forEach((key, value) -> record.headers()
                        .add(key, value.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record)
                .join();
    }

    private ConsumerRecord<String, String> awaitRecord(String topic, String key) {
        return await().atMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> recordsFor(topic, key), records -> !records.isEmpty())
                .getFirst();
    }

    /**
     * Tous les messages du topic portant cette cle, lus du debut jusqu'a la fin.
     *
     * <p>{@code assign} plutot que {@code subscribe} : pas de consumer group,
     * donc pas d'attente de rebalance, et une lecture qui s'arrete au dernier offset.
     */
    private List<ConsumerRecord<String, String>> recordsFor(String topic, String key) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(brokers, "embedded-test", false);
        try (Consumer<String, String> consumer =
                     new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic)
                    .stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            while (partitions.stream()
                    .anyMatch(tp -> consumer.position(tp) < endOffsets.get(tp))) {
                consumer.poll(Duration.ofMillis(100))
                        .records(topic)
                        .forEach(record -> {
                            if (key.equals(record.key())) {
                                records.add(record);
                            }
                        });
            }
            return records;
        }
    }
}
