package fr.orderflow.order.service;

import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.test.KafkaTestSupport;
import fr.orderflow.order.api.CreateOrderRequest;
import fr.orderflow.order.domain.OrderEntity;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 5 : la trace distribuee ne s'arrete pas a l'outbox.
 *
 * <p>L'outbox decouple l'ecriture de la commande (dans la requete) de la publication Kafka (plus tard,
 * par le relais, hors de toute requete). Le test ouvre un span comme le ferait la requete HTTP, cree la
 * commande, ferme le span, puis declenche le relais <i>apres coup</i>, sans span courant : le message
 * publie doit quand meme porter le {@code traceId} de la requete d'origine.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.observation-enabled=true",
        "spring.kafka.template.observation-enabled=true",
        "management.tracing.sampling.probability=1.0"})
@EmbeddedKafka(partitions = 3, topics = {
        Topics.ORDERS_CREATED, Topics.ORDERS_CONFIRMED, Topics.ORDERS_CANCELLED,
        Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED,
        Topics.PAYMENTS_COMPLETED, Topics.PAYMENTS_FAILED})
@DirtiesContext
class EmbeddedKafkaOrderObservabilityTest {

    private static final String CID = "corr-trace";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private OrderService orderService;
    @Autowired
    private OutboxRelay outboxRelay;
    @Autowired
    private Tracer tracer;

    private KafkaTestSupport kafka;

    @BeforeEach
    void setUp() {
        kafka = new KafkaTestSupport(brokers);
    }

    @Test
    @DisplayName("Le traceId de la requete se retrouve sur le message publie plus tard par le relais d'outbox")
    void traceOfTheRequest_continuesThroughTheOutboxToKafka() {
        Span requestSpan = tracer.nextSpan().name("POST /api/orders (simule)").start();
        String requestTraceId = requestSpan.context().traceId();
        OrderEntity order;
        try (Tracer.SpanInScope ignored = tracer.withSpan(requestSpan)) {
            order = orderService.createOrder(new CreateOrderRequest(
                    "cust-118", List.of(new CreateOrderRequest.Item("sku-001", 2))), CID);
        } finally {
            requestSpan.end();
        }
        assertThat(tracer.currentSpan()).as("la requete est terminee, plus de span courant").isNull();

        outboxRelay.publishPending();

        ConsumerRecord<String, byte[]> record = kafka.awaitRecord(Topics.ORDERS_CREATED, order.getId());
        String traceparent = KafkaTestSupport.header(record, "traceparent");
        assertThat(traceparent)
                .as("traceparent du message (en-tetes : %s)", KafkaTestSupport.describeHeaders(record))
                .isNotNull()
                .contains(requestTraceId);
        assertThat(KafkaTestSupport.header(record, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
    }
}
