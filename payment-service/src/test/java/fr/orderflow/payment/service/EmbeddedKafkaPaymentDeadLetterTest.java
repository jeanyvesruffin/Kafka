package fr.orderflow.payment.service;

import fr.orderflow.common.event.InventoryReservedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.test.KafkaTestSupport;
import fr.orderflow.payment.repository.PaymentRepository;
import io.micrometer.core.instrument.MeterRegistry;
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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Phase 4 : un prestataire qui ne revient pas. Le message epuise ses tentatives (origine,
 * {@code -retry-0}, {@code -retry-1}) puis part sur {@code inventory.reserved.DLT}, sans bloquer
 * la partition ni produire de resultat de paiement.
 *
 * <p>{@code transient-failures-per-order=99} : bien au-dela des 3 tentatives.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "orderflow.payment.transient-failures-per-order=99"})
@EmbeddedKafka(partitions = 3, topics = {
        Topics.INVENTORY_RESERVED, Topics.PAYMENTS_COMPLETED, Topics.PAYMENTS_FAILED})
@DirtiesContext
class EmbeddedKafkaPaymentDeadLetterTest {

    private static final String CID = "corr-dlt";
    private static final String RETRY_0 = Topics.INVENTORY_RESERVED + "-retry-0";
    private static final String RETRY_1 = Topics.INVENTORY_RESERVED + "-retry-1";
    private static final String DLT = Topics.dltOf(Topics.INVENTORY_RESERVED);

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private EventSerializer eventSerializer;
    @Autowired
    private PaymentRepository paymentRepository;
    @Autowired
    private MeterRegistry meterRegistry;

    private KafkaTestSupport kafka;

    @BeforeEach
    void setUp() {
        kafka = new KafkaTestSupport(brokers);
    }

    @Test
    @DisplayName("Prestataire indisponible en permanence : retry-0, retry-1 puis DLT, aucun resultat de paiement")
    void permanentGatewayFailure_endsInDeadLetterTopic() {
        BigDecimal total = new BigDecimal("39.80");
        InventoryReservedEvent event = new InventoryReservedEvent(
                UUID.randomUUID().toString(), "ord-" + UUID.randomUUID(), "cust-118",
                List.of(new OrderLine("sku-001", 1, total)), total, Instant.now());
        EventEnvelope envelope = eventSerializer.envelope(Topics.INVENTORY_RESERVED, event, CID);

        kafka.send(envelope.topic(), envelope.key(), envelope.payload(), envelope.headers());

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(DLT, event.orderId());
        // Les topics de retry posent leurs en-tetes sous kafka_exception-* (pas kafka_dlt-*)
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.EXCEPTION_CAUSE_FQCN))
                .as(KafkaTestSupport.describeHeaders(dead))
                .contains("PaymentGatewayUnavailableException");
        // L'origine reste le topic d'entree, pas le dernier topic de retry traverse
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.ORIGINAL_TOPIC)).isEqualTo(Topics.INVENTORY_RESERVED);
        assertThat(KafkaTestSupport.header(dead, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        assertThat(KafkaTestSupport.text(dead)).isEqualTo(envelope.payload());   // message intact, rejouable

        assertThat(kafka.records(RETRY_0, event.orderId())).hasSize(1);
        assertThat(kafka.records(RETRY_1, event.orderId())).hasSize(1);
        assertThat(kafka.records(DLT, event.orderId())).hasSize(1);

        // Aucun encaissement, aucun evenement de resultat : la commande reste en attente d'une decision.
        assertThat(paymentRepository.findByOrderId(event.orderId())).isEmpty();
        assertThat(kafka.records(Topics.PAYMENTS_COMPLETED, event.orderId())).isEmpty();
        assertThat(kafka.records(Topics.PAYMENTS_FAILED, event.orderId())).isEmpty();

        // Le compteur a surveiller (le handler de DLT le leve apres la lecture du DLT).
        await().atMost(KafkaTestSupport.TIMEOUT)
                .untilAsserted(() -> assertThat(meterRegistry.find("orderflow.kafka.dlt")
                        .tag("topic", Topics.INVENTORY_RESERVED)
                        .tag("exception", "PaymentGatewayUnavailableException")
                        .counter()).isNotNull());
    }

    @Test
    @DisplayName("Message empoisonne (JSON illisible) : DLT immediat, sans passer par les topics de retry")
    void poisonPill_goesStraightToDeadLetterTopic() {
        String orderId = "ord-poison-" + UUID.randomUUID();
        String garbage = "{\"eventId\": \"abc\", ceci n'est pas du JSON";

        kafka.send(Topics.INVENTORY_RESERVED, orderId, garbage, Map.of(EventHeaders.CORRELATION_ID, CID));

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(DLT, orderId);
        assertThat(KafkaTestSupport.text(dead)).isEqualTo(garbage);
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.EXCEPTION_CAUSE_FQCN)).contains("jackson");
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.ORIGINAL_TOPIC)).isEqualTo(Topics.INVENTORY_RESERVED);
        // Reessayer ne guerira pas un JSON illisible : ni -retry-0 ni -retry-1.
        assertThat(kafka.records(RETRY_0, orderId)).isEmpty();
        assertThat(kafka.records(RETRY_1, orderId)).isEmpty();
    }
}
