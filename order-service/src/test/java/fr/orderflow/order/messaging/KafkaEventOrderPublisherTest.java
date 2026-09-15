package fr.orderflow.order.messaging;

import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.Topics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests unitaires purs du publisher Kafka — aucun broker, le {@link KafkaTemplate} est bouchonne.
 */
class KafkaEventOrderPublisherTest {

    private static final EventEnvelope ENVELOPE = new EventEnvelope(
            Topics.ORDERS_CREATED, "ord-1", "{}", Map.of(EventHeaders.CORRELATION_ID, "corr-test"));

    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaEventOrderPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        publisher = new KafkaEventOrderPublisher(kafkaTemplate);
    }

    @Test
    @DisplayName("Message acquitte par le broker : publish rend la main, cle et en-tetes transmis")
    @SuppressWarnings("unchecked")
    void acknowledgedSend_returnsNormally() {
        Mockito.when(kafkaTemplate.send(Mockito.<ProducerRecord<String, String>>any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(ENVELOPE);

        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        Mockito.verify(kafkaTemplate)
                .send(captor.capture());
        ProducerRecord<String, String> record = captor.getValue();
        assertThat(record.topic()).isEqualTo(Topics.ORDERS_CREATED);
        assertThat(record.key()).isEqualTo("ord-1");
        assertThat(new String(record.headers()
                .lastHeader(EventHeaders.CORRELATION_ID)
                .value(), StandardCharsets.UTF_8)).isEqualTo("corr-test");
    }

    @Test
    @DisplayName("Echec asynchrone du broker : publish leve une exception, le relais ne marquera pas la ligne publiee")
    void failedSend_throws() {
        Mockito.when(kafkaTemplate.send(Mockito.<ProducerRecord<String, String>>any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker indisponible")));

        assertThatThrownBy(() -> publisher.publish(ENVELOPE))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining(Topics.ORDERS_CREATED)
                .hasMessageContaining("broker indisponible");
    }
}
