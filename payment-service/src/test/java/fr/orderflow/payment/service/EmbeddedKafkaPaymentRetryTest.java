package fr.orderflow.payment.service;

import fr.orderflow.common.event.InventoryReservedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.test.KafkaTestSupport;
import fr.orderflow.payment.domain.PaymentEntity;
import fr.orderflow.payment.domain.PaymentStatus;
import fr.orderflow.payment.repository.PaymentRepository;
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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 4 : reprises non bloquantes de payment-service, face a une panne transitoire du prestataire.
 *
 * <p>{@code orderflow.payment.transient-failures-per-order=2} fait echouer les deux premiers appels de
 * chaque commande. Avec 3 tentatives (origine + {@code -retry-0} + {@code -retry-1}), le troisieme appel
 * reussit : le message doit avoir transite par les deux topics de retry, jamais par le DLT, et le
 * paiement ne doit etre enregistre qu'une fois.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "orderflow.payment.transient-failures-per-order=2"})
@EmbeddedKafka(partitions = 3, topics = {
        Topics.INVENTORY_RESERVED, Topics.PAYMENTS_COMPLETED, Topics.PAYMENTS_FAILED})
@DirtiesContext
class EmbeddedKafkaPaymentRetryTest {

    private static final String CID = "corr-retry";
    private static final String RETRY_0 = Topics.INVENTORY_RESERVED + "-retry-0";
    private static final String RETRY_1 = Topics.INVENTORY_RESERVED + "-retry-1";
    private static final String DLT = Topics.dltOf(Topics.INVENTORY_RESERVED);

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private EventSerializer eventSerializer;
    @Autowired
    private PaymentRepository paymentRepository;

    private KafkaTestSupport kafka;

    @BeforeEach
    void setUp() {
        kafka = new KafkaTestSupport(brokers);
    }

    @Test
    @DisplayName("Panne transitoire : le message transite par -retry-0 puis -retry-1, le paiement aboutit une seule fois")
    void transientGatewayFailure_isRetriedThroughRetryTopics_thenCompleted() {
        InventoryReservedEvent event = reserved("39.80");

        publish(event);

        ConsumerRecord<String, byte[]> completed = kafka.awaitRecord(Topics.PAYMENTS_COMPLETED, event.orderId());
        assertThat(KafkaTestSupport.header(completed, "correlationId")).isEqualTo(CID);

        // Le meme message est passe par les deux topics de retry, dans cet ordre, une fois chacun...
        assertThat(kafka.records(RETRY_0, event.orderId())).hasSize(1);
        assertThat(kafka.records(RETRY_1, event.orderId())).hasSize(1);
        // ... et le correlationId a suivi le message jusque-la.
        assertThat(KafkaTestSupport.header(kafka.records(RETRY_1, event.orderId()).getFirst(), "correlationId"))
                .isEqualTo(CID);
        // Il n'est jamais alle en DLT.
        assertThat(kafka.records(DLT, event.orderId())).isEmpty();

        // Les deux premiers appels ont annule leur transaction (processed_events compris) : un seul
        // paiement, une seule publication.
        assertThat(paymentRepository.findByOrderId(event.orderId()))
                .extracting(PaymentEntity::getStatus)
                .containsExactly(PaymentStatus.COMPLETED);
        assertThat(kafka.records(Topics.PAYMENTS_COMPLETED, event.orderId())).hasSize(1);
        assertThat(kafka.records(Topics.PAYMENTS_FAILED, event.orderId())).isEmpty();
    }

    // ------------------------------------------------------------------

    private InventoryReservedEvent reserved(String amount) {
        BigDecimal total = new BigDecimal(amount);
        return new InventoryReservedEvent(
                UUID.randomUUID().toString(), "ord-" + UUID.randomUUID(), "cust-118",
                List.of(new OrderLine("sku-001", 1, total)), total, Instant.now());
    }

    private void publish(InventoryReservedEvent event) {
        EventEnvelope envelope = eventSerializer.envelope(Topics.INVENTORY_RESERVED, event, CID);
        kafka.send(envelope.topic(), envelope.key(), envelope.payload(), envelope.headers());
    }
}
