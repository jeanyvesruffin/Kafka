package fr.orderflow.order.messaging;

import fr.orderflow.common.event.OrderConfirmedEvent;
import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.messaging.avro.OrderCreatedAvroCodec;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.json.JsonMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests unitaires purs du publisher Kafka — aucun broker, le {@link KafkaTemplate} est bouchonne.
 */
class KafkaEventOrderPublisherTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:15:30Z");

    private final EventSerializer eventSerializer = new EventSerializer(JsonMapper.builder().build());
    private final OrderCreatedAvroCodec avroCodec = new OrderCreatedAvroCodec();

    private final OrderCreatedEvent orderCreated = new OrderCreatedEvent(
            "evt-1", "ord-1", "cust-118",
            List.of(new OrderLine("sku-001", 2, new BigDecimal("19.90"))),
            new BigDecimal("39.80"), NOW, "PROMO10");

    private EventEnvelope orderCreatedEnvelope;
    private KafkaTemplate<String, Object> kafkaTemplate;
    private KafkaEventOrderPublisher publisher;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        kafkaTemplate = Mockito.mock(KafkaTemplate.class);
        publisher = new KafkaEventOrderPublisher(kafkaTemplate, eventSerializer, avroCodec);
        orderCreatedEnvelope = new EventEnvelope(
                Topics.ORDERS_CREATED, "ord-1", eventSerializer.toJson(orderCreated),
                Map.of(EventHeaders.CORRELATION_ID, "corr-test", EventHeaders.CONTENT_TYPE, "application/json"));
    }

    @SuppressWarnings("unchecked")
    private ProducerRecord<String, Object> sentRecord() {
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        Mockito.verify(kafkaTemplate).send(captor.capture());
        return captor.getValue();
    }

    private static String header(ProducerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Message acquitte par le broker : publish rend la main, cle et en-tetes transmis")
    void acknowledgedSend_returnsNormally() {
        Mockito.when(kafkaTemplate.send(Mockito.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(orderCreatedEnvelope);

        ProducerRecord<String, Object> record = sentRecord();
        assertThat(record.topic()).isEqualTo(Topics.ORDERS_CREATED);
        assertThat(record.key()).isEqualTo("ord-1");
        assertThat(header(record, EventHeaders.CORRELATION_ID)).isEqualTo("corr-test");
    }

    @Test
    @DisplayName("orders.created part en AVRO (octets C3 01...), l'en-tete contentType l'annonce, l'evenement est intact")
    void ordersCreated_isPublishedAsAvro() throws IOException {
        Mockito.when(kafkaTemplate.send(Mockito.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(orderCreatedEnvelope);

        ProducerRecord<String, Object> record = sentRecord();
        assertThat(record.value()).isInstanceOf(byte[].class);
        byte[] bytes = (byte[]) record.value();
        assertThat(bytes[0]).isEqualTo((byte) 0xC3);
        assertThat(bytes[1]).isEqualTo((byte) 0x01);
        assertThat(header(record, EventHeaders.CONTENT_TYPE)).isEqualTo("application/avro");
        assertThat(avroCodec.decode(bytes)).isEqualTo(orderCreated);   // coupon compris
    }

    @Test
    @DisplayName("Les autres topics restent en JSON (String), contentType inchange")
    void otherTopics_stayJson() {
        Mockito.when(kafkaTemplate.send(Mockito.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.completedFuture(null));
        String json = eventSerializer.toJson(new OrderConfirmedEvent("evt-2", "ord-1", NOW));
        EventEnvelope confirmed = new EventEnvelope(
                Topics.ORDERS_CONFIRMED, "ord-1", json,
                Map.of(EventHeaders.CORRELATION_ID, "corr-test", EventHeaders.CONTENT_TYPE, "application/json"));

        publisher.publish(confirmed);

        ProducerRecord<String, Object> record = sentRecord();
        assertThat(record.value()).isEqualTo(json);
        assertThat(header(record, EventHeaders.CONTENT_TYPE)).isEqualTo("application/json");
    }

    @Test
    @DisplayName("Echec asynchrone du broker : publish leve une exception, le relais ne marquera pas la ligne publiee")
    void failedSend_throws() {
        Mockito.when(kafkaTemplate.send(Mockito.<ProducerRecord<String, Object>>any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker indisponible")));

        assertThatThrownBy(() -> publisher.publish(orderCreatedEnvelope))
                .isInstanceOf(KafkaException.class)
                .hasMessageContaining(Topics.ORDERS_CREATED)
                .hasMessageContaining("broker indisponible");
    }
}
