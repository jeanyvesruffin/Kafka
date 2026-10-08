package fr.orderflow.notification.messaging;

import fr.orderflow.common.event.OrderConfirmedEvent;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.test.KafkaTestSupport;
import fr.orderflow.notification.service.NotificationService;
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

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests d'integration Kafka du service Notification, sur un broker embarque : parcours nominal et
 * Dead Letter Topic (phase 4).
 *
 * <p>Ce service est sans etat ni base : {@link NotificationService} est espionne pour observer ce
 * que le listener lui delegue.
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {Topics.ORDERS_CONFIRMED, Topics.ORDERS_CANCELLED})
@DirtiesContext
class EmbeddedKafkaNotificationTest {

    private static final String CID = "corr-notif";

    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private EventSerializer eventSerializer;
    @MockitoSpyBean
    private NotificationService notificationService;

    private KafkaTestSupport kafka;

    @BeforeEach
    void setUp() {
        kafka = new KafkaTestSupport(brokers);
    }

    @Test
    @DisplayName("OrderConfirmed : le listener delegue la notification, avec le correlationId de l'en-tete")
    void orderConfirmed_notifiesCustomer() {
        var event = new OrderConfirmedEvent(UUID.randomUUID().toString(), "ord-" + UUID.randomUUID(), Instant.now());
        EventEnvelope envelope = eventSerializer.envelope(Topics.ORDERS_CONFIRMED, event, CID);

        kafka.send(envelope.topic(), envelope.key(), envelope.payload(), envelope.headers());

        await().atMost(KafkaTestSupport.TIMEOUT)
                .untilAsserted(() -> verify(notificationService).notifyCustomer(any(OrderConfirmedEvent.class), eq(CID)));
        assertThat(kafka.records(Topics.dltOf(Topics.ORDERS_CONFIRMED), event.orderId())).isEmpty();
    }

    @Test
    @DisplayName("Type d'evenement inattendu : DLT immediat, aucune notification")
    void unexpectedEventType_goesToDeadLetterTopic() {
        String orderId = "ord-" + UUID.randomUUID();
        Map<String, String> headers = Map.of(EventHeaders.EVENT_TYPE, "PaymentCompleted", EventHeaders.CORRELATION_ID, CID);

        kafka.send(Topics.ORDERS_CONFIRMED, orderId, "{}", headers);

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(Topics.dltOf(Topics.ORDERS_CONFIRMED), orderId);
        assertThat(KafkaTestSupport.header(dead, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)).contains("IllegalArgumentException");
        verify(notificationService, never()).notifyCustomer(any(), any());
    }

    @Test
    @DisplayName("JSON illisible : DLT immediat, aucune notification")
    void malformedJson_goesToDeadLetterTopic() {
        String orderId = "ord-" + UUID.randomUUID();
        String garbage = "{\"eventId\": ";
        Map<String, String> headers = Map.of(EventHeaders.EVENT_TYPE, OrderConfirmedEvent.TYPE, EventHeaders.CORRELATION_ID, CID);

        kafka.send(Topics.ORDERS_CONFIRMED, orderId, garbage, headers);

        ConsumerRecord<String, byte[]> dead = kafka.awaitRecord(Topics.dltOf(Topics.ORDERS_CONFIRMED), orderId);
        assertThat(KafkaTestSupport.text(dead)).isEqualTo(garbage);
        verify(notificationService, never()).notifyCustomer(any(), any());
    }
}
