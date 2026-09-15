package fr.orderflow.payment.service;

import fr.orderflow.common.event.*;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.payment.domain.PaymentEntity;
import fr.orderflow.payment.domain.PaymentStatus;
import fr.orderflow.payment.repository.PaymentRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
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
 * Tests d'integration Kafka du service Paiement, sur un broker embarque.
 *
 * <p>Le test joue le role d'inventory-service : il publie sur
 * {@code inventory.reserved} exactement ce que Inventory publierait (payload
 * JSON + en-tetes), puis observe ce que Payment publie en retour et le
 * paiement enregistre en base.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {
        Topics.INVENTORY_RESERVED, Topics.PAYMENTS_COMPLETED, Topics.PAYMENTS_FAILED})
@DirtiesContext
class EmbeddedKafkaPaymentTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String CID = "corr-embedded";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private EventSerializer eventSerializer;
    @Autowired
    private PaymentRepository paymentRepository;

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers()
                .lastHeader(name)
                .value(), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Montant sous le plafond : PaymentCompleted publie (cle et correlationId conserves), paiement COMPLETED")
    void underThreshold_publishesPaymentCompleted() {
        InventoryReservedEvent event = reserved("39.80");

        publish(Topics.INVENTORY_RESERVED, event);

        ConsumerRecord<String, String> record = awaitRecord(Topics.PAYMENTS_COMPLETED, event.orderId());
        assertThat(header(record, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        assertThat(header(record, EventHeaders.EVENT_TYPE)).isEqualTo(PaymentCompletedEvent.TYPE);
        PaymentCompletedEvent completed = eventSerializer.fromJson(record.value(), PaymentCompletedEvent.class);
        assertThat(completed.amount()).isEqualByComparingTo("39.80");

        await().atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(paymentRepository.findByOrderId(event.orderId()))
                        .extracting(PaymentEntity::getStatus)
                        .containsExactly(PaymentStatus.COMPLETED));
    }

    @Test
    @DisplayName("Montant au-dessus du plafond : PaymentFailed publie avec le motif du refus")
    void aboveThreshold_publishesPaymentFailed() {
        InventoryReservedEvent event = reserved("1250.00");

        publish(Topics.INVENTORY_RESERVED, event);

        ConsumerRecord<String, String> record = awaitRecord(Topics.PAYMENTS_FAILED, event.orderId());
        assertThat(header(record, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        PaymentFailedEvent failed = eventSerializer.fromJson(record.value(), PaymentFailedEvent.class);
        assertThat(failed.reason()).contains("Plafond depasse");
        assertThat(recordsFor(Topics.PAYMENTS_COMPLETED, event.orderId())).isEmpty();
    }

    @Test
    @DisplayName("Un InventoryReserved redelivre (meme eventId) ne debite qu'une fois (idempotence)")
    void redeliveredInventoryReserved_chargesOnlyOnce() {
        InventoryReservedEvent event = reserved("39.80");

        publish(Topics.INVENTORY_RESERVED, event);
        publish(Topics.INVENTORY_RESERVED, event);   // redelivery apres rebalance

        awaitRecord(Topics.PAYMENTS_COMPLETED, event.orderId());
        // Meme cle => meme partition : le doublon est consomme juste apres l'original.
        await().during(Duration.ofSeconds(3))
                .atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(paymentRepository.findByOrderId(event.orderId())).hasSize(1));
        assertThat(recordsFor(Topics.PAYMENTS_COMPLETED, event.orderId())).hasSize(1);
    }

    // ------------------------------------------------------------------

    private InventoryReservedEvent reserved(String amount) {
        BigDecimal total = new BigDecimal(amount);
        return new InventoryReservedEvent(
                UUID.randomUUID()
                        .toString(), "ord-" + UUID.randomUUID(), "cust-118",
                List.of(new OrderLine("sku-001", 1, total)), total, Instant.now());
    }

    /**
     * Publie l'evenement tel que le ferait inventory-service : cle = orderId, en-tetes standards.
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
