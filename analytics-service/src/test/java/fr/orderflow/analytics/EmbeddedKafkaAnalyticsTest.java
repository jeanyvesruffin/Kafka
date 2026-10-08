package fr.orderflow.analytics;

import fr.orderflow.analytics.api.AnalyticsQueries;
import fr.orderflow.common.event.OrderCancelledEvent;
import fr.orderflow.common.event.OrderConfirmedEvent;
import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderFlowEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.event.PaymentCompletedEvent;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.test.KafkaTestSupport;
import fr.orderflow.common.test.OrderCreatedMessages;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 8 et 9 : l'application Analytics complete sur un broker embarque, avec le profil de production
 * ({@code processing.guarantee: exactly_once_v2} de l'{@code application.yml}).
 *
 * <p>Le test publie des evenements comme le feraient les autres services, puis interroge l'API des
 * state stores. Chaque test utilise ses propres commandes : les agregats sont cumulatifs, on compare
 * donc des <i>differences</i> avant / apres plutot que des valeurs absolues.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.state-dir=${java.io.tmpdir}/orderflow-analytics-test-${random.uuid}",
        // Un seul broker embarque : les topics internes de la transaction n'ont qu'une replique
        "spring.kafka.streams.properties.replication.factor=1",
        "spring.kafka.streams.properties.commit.interval.ms=200"})
@EmbeddedKafka(partitions = 3,
        topics = {Topics.ORDERS_CREATED, Topics.ORDERS_CONFIRMED, Topics.ORDERS_CANCELLED, Topics.PAYMENTS_COMPLETED},
        brokerProperties = {"transaction.state.log.replication.factor=1", "transaction.state.log.min.isr=1"})
@DirtiesContext
class EmbeddedKafkaAnalyticsTest {

    private static final String CID = "corr-analytics";
    private static final String DLQ = "orderflow-analytics.DLT";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private AnalyticsQueries queries;
    @Autowired
    private EventSerializer eventSerializer;

    private KafkaTestSupport kafka;

    @BeforeEach
    void setUp() {
        kafka = new KafkaTestSupport(brokers);
    }

    private static OrderCreatedEvent orderCreated(String orderId) {
        return new OrderCreatedEvent(
                UUID.randomUUID().toString(), orderId, "cust-118",
                List.of(new OrderLine("sku-001", 2, new BigDecimal("19.90"))), new BigDecimal("39.80"), Instant.now());
    }

    private void publishJson(String topic, OrderFlowEvent event) {
        EventEnvelope envelope = eventSerializer.envelope(topic, event, CID);
        kafka.send(envelope.topic(), envelope.key(), envelope.payload(), envelope.headers());
    }

    private long count(String status) {
        return queries.ordersByStatus().get(status);
    }

    @Test
    @DisplayName("Parcours complet : commandes par statut et chiffre d'affaires suivent les evenements (exactly_once_v2)")
    void aggregates_followTheEvents() {
        await().atMost(KafkaTestSupport.TIMEOUT).ignoreExceptions()
                .untilAsserted(() -> assertThat(queries.ordersByStatus()).isNotNull());
        long createdBefore = count("CREATED");
        long confirmedBefore = count("CONFIRMED");
        long cancelledBefore = count("CANCELLED");
        BigDecimal revenueBefore = queries.revenue();
        String confirmed = "ord-confirmed-" + UUID.randomUUID();
        String cancelled = "ord-cancelled-" + UUID.randomUUID();
        String pending = "ord-pending-" + UUID.randomUUID();

        for (String orderId : List.of(confirmed, cancelled, pending)) {
            OrderCreatedMessages.publish(kafka, orderCreated(orderId), CID);
        }
        publishJson(Topics.ORDERS_CONFIRMED, new OrderConfirmedEvent(UUID.randomUUID().toString(), confirmed, Instant.now()));
        publishJson(Topics.PAYMENTS_COMPLETED, new PaymentCompletedEvent(
                UUID.randomUUID().toString(), confirmed, new BigDecimal("39.80"), "tx-1", Instant.now()));
        publishJson(Topics.ORDERS_CANCELLED, new OrderCancelledEvent(
                UUID.randomUUID().toString(), cancelled, List.of(), "Plafond depasse", Instant.now()));

        await().atMost(KafkaTestSupport.TIMEOUT).pollInterval(java.time.Duration.ofMillis(300)).ignoreExceptions()
                .untilAsserted(() -> {
                    assertThat(count("CREATED")).isEqualTo(createdBefore + 1);       // seule 'pending' est en cours
                    assertThat(count("CONFIRMED")).isEqualTo(confirmedBefore + 1);
                    assertThat(count("CANCELLED")).isEqualTo(cancelledBefore + 1);
                    assertThat(queries.revenue()).isEqualByComparingTo(revenueBefore.add(new BigDecimal("39.80")));
                });
    }

    @Test
    @DisplayName("Message illisible sur orders.confirmed : l'application continue, le message est en DLQ")
    void poisonPill_goesToDeadLetterQueue_andTheStreamContinues() {
        await().atMost(KafkaTestSupport.TIMEOUT).ignoreExceptions()
                .untilAsserted(() -> assertThat(queries.ordersByStatus()).isNotNull());
        long confirmedBefore = count("CONFIRMED");
        String poison = "ord-poison-" + UUID.randomUUID();
        String valid = "ord-valid-" + UUID.randomUUID();

        kafka.send(Topics.ORDERS_CONFIRMED, poison, "ceci n'est pas du JSON", Map.of(EventHeaders.CORRELATION_ID, CID));
        publishJson(Topics.ORDERS_CONFIRMED, new OrderConfirmedEvent(UUID.randomUUID().toString(), valid, Instant.now()));

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(DLQ, poison);
        assertThat(KafkaTestSupport.text(dead)).isEqualTo("ceci n'est pas du JSON");
        await().atMost(KafkaTestSupport.TIMEOUT).ignoreExceptions()
                .untilAsserted(() -> assertThat(count("CONFIRMED")).isEqualTo(confirmedBefore + 1));
    }
}
