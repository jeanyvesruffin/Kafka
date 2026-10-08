package fr.orderflow.order.service;

import fr.orderflow.common.event.PaymentCompletedEvent;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.test.KafkaTestSupport;
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
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Phase 4 : Dead Letter Topic d'order-service.
 *
 * <p>Deux erreurs qui ne guerissent pas en reessayant : un evenement pour une commande inconnue
 * ({@link OrderNotFoundException}, base reinitialisee alors que Kafka a conserve les messages) et un
 * JSON illisible. Dans les deux cas : un seul appel, puis {@code <topic>.DLT}.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {
        Topics.ORDERS_CREATED, Topics.ORDERS_CONFIRMED, Topics.ORDERS_CANCELLED,
        Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED,
        Topics.PAYMENTS_COMPLETED, Topics.PAYMENTS_FAILED})
@DirtiesContext
class EmbeddedKafkaOrderDeadLetterTest {

    private static final String CID = "corr-dlt";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private EventSerializer eventSerializer;
    @MockitoSpyBean
    private OrderService orderService;

    private KafkaTestSupport kafka;

    @BeforeEach
    void setUp() {
        kafka = new KafkaTestSupport(brokers);
    }

    @Test
    @DisplayName("Evenement pour une commande inconnue : un seul appel (non retryable), puis DLT")
    void unknownOrder_goesToDeadLetterTopic_withoutRetry() {
        String orderId = "ord-absent-" + UUID.randomUUID();
        var event = new PaymentCompletedEvent(
                UUID.randomUUID().toString(), orderId, new BigDecimal("39.80"), "tx-42", Instant.now());
        EventEnvelope envelope = eventSerializer.envelope(Topics.PAYMENTS_COMPLETED, event, CID);

        kafka.send(envelope.topic(), envelope.key(), envelope.payload(), envelope.headers());

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(Topics.dltOf(Topics.PAYMENTS_COMPLETED), orderId);
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).contains("OrderNotFoundException");
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isEqualTo("order-service");
        verify(orderService, times(1)).onPaymentCompleted(eq(orderId), anyString());
        // Aucune commande n'a ete confirmee.
        assertThat(kafka.records(Topics.ORDERS_CONFIRMED, orderId)).isEmpty();
    }

    @Test
    @DisplayName("Message empoisonne (JSON illisible) sur inventory.rejected : DLT immediat, le service n'est jamais appele")
    void malformedJson_goesToDeadLetterTopic() {
        String orderId = "ord-poison-" + UUID.randomUUID();
        String garbage = "ceci n'est pas du JSON";

        kafka.send(Topics.INVENTORY_REJECTED, orderId, garbage, Map.of(EventHeaders.CORRELATION_ID, CID));

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(Topics.dltOf(Topics.INVENTORY_REJECTED), orderId);
        assertThat(KafkaTestSupport.text(dead)).isEqualTo(garbage);
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).contains("jackson");
        verify(orderService, never()).onInventoryRejected(any(), any(), any());
    }
}
