package fr.orderflow.inventory.service;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.CorrelationId;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.test.KafkaTestSupport;
import fr.orderflow.common.test.OrderCreatedMessages;
import fr.orderflow.inventory.domain.StockEntity;
import fr.orderflow.inventory.repository.StockRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

/**
 * Phase 5 : observabilite d'inventory-service, sur un broker embarque.
 *
 * <ul>
 *   <li>le {@code correlationId} de l'en-tete Kafka est dans le MDC pendant le traitement ;</li>
 *   <li>le contexte de trace W3C ({@code traceparent}) recu avec le message est repris par le consumer
 *       et reecrit sur le message publie : un seul {@code traceId} de bout en bout.</li>
 * </ul>
 *
 * <p>Le profil de test ne reprend pas l'{@code application.yml} principal ; l'observation Kafka et
 * l'echantillonnage a 100 % sont donc actives ici explicitement.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.listener.observation-enabled=true",
        "spring.kafka.template.observation-enabled=true",
        "management.tracing.sampling.probability=1.0"})
@EmbeddedKafka(partitions = 3, topics = {
        Topics.ORDERS_CREATED, Topics.ORDERS_CANCELLED,
        Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED})
@DirtiesContext
class EmbeddedKafkaInventoryObservabilityTest {

    private static final String CID = "corr-observability";
    private static final String TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736";
    private static final String UPSTREAM_SPAN_ID = "00f067aa0ba902b7";

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
    @DisplayName("Le correlationId de l'en-tete Kafka est dans le MDC pendant le traitement du message")
    void correlationId_isInMdcDuringProcessing() {
        AtomicReference<String> mdcDuringProcessing = new AtomicReference<>();
        doAnswer(invocation -> {
            mdcDuringProcessing.set(MDC.get(CorrelationId.MDC_KEY));
            return invocation.callRealMethod();
        }).when(inventoryService).handleOrderCreated(any(), any());
        OrderCreatedEvent event = orderCreated();

        send(event, Map.of());

        kafka.awaitRecord(Topics.INVENTORY_RESERVED, event.orderId());
        assertThat(mdcDuringProcessing).hasValue(CID);
    }

    @Test
    @DisplayName("Le traceparent recu est repris par le consumer et reecrit sur le message publie (meme traceId)")
    void traceContext_isPropagatedToThePublishedMessage() {
        OrderCreatedEvent event = orderCreated();
        String incoming = "00-" + TRACE_ID + "-" + UPSTREAM_SPAN_ID + "-01";

        send(event, Map.of("traceparent", incoming));

        ConsumerRecord<String, byte[]> reserved = kafka.awaitRecord(Topics.INVENTORY_RESERVED, event.orderId());
        String outgoing = KafkaTestSupport.header(reserved, "traceparent");
        assertThat(outgoing)
                .as("traceparent du message publie (en-tetes : %s)", KafkaTestSupport.describeHeaders(reserved))
                .isNotNull()
                .contains(TRACE_ID);
        // Meme trace, mais un autre segment : le span d'inventory-service, enfant du span recu.
        assertThat(outgoing).doesNotContain(UPSTREAM_SPAN_ID);
        // Le correlationId, lui, voyage toujours dans son propre en-tete.
        assertThat(KafkaTestSupport.header(reserved, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
    }

    // ------------------------------------------------------------------

    private OrderCreatedEvent orderCreated() {
        OrderLine line = new OrderLine(productId, 1, new BigDecimal("19.90"));
        return new OrderCreatedEvent(
                UUID.randomUUID().toString(), "ord-" + UUID.randomUUID(), "cust-118",
                List.of(line), line.lineTotal(), Instant.now());
    }

    private void send(OrderCreatedEvent event, Map<String, String> extraHeaders) {
        OrderCreatedMessages.publish(kafka, event, CID, extraHeaders);   // orders.created est en Avro (phase 7)
    }
}
