package fr.orderflow.common.test;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.messaging.avro.OrderCreatedAvroCodec;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Publie des {@code OrderCreated} sur {@code orders.created} comme le ferait order-service, c'est-a-dire
 * en Avro (phase 7) : cle = orderId, en-tetes standards, {@code contentType} {@code application/avro}.
 */
public final class OrderCreatedMessages {

    private static final OrderCreatedAvroCodec CODEC = new OrderCreatedAvroCodec();

    private OrderCreatedMessages() {
    }

    /**
     * Producteur de la version courante du schema.
     */
    public static void publish(KafkaTestSupport kafka, OrderCreatedEvent event, String correlationId) {
        publish(kafka, event, correlationId, Map.of());
    }

    public static void publish(
            KafkaTestSupport kafka, OrderCreatedEvent event, String correlationId, Map<String, String> extraHeaders) {
        kafka.send(Topics.ORDERS_CREATED, event.orderId(), CODEC.encode(event), headers(event, correlationId, extraHeaders));
    }

    /**
     * Producteur de la version 1 du schema, pas encore redeploye (le coupon n'existe pas pour lui).
     */
    public static void publishAsVersion1(KafkaTestSupport kafka, OrderCreatedEvent event, String correlationId) {
        kafka.send(Topics.ORDERS_CREATED, event.orderId(), OrderCreatedV1.encode(event),
                headers(event, correlationId, Map.of()));
    }

    private static Map<String, String> headers(
            OrderCreatedEvent event, String correlationId, Map<String, String> extraHeaders) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(EventHeaders.EVENT_ID, event.eventId());
        headers.put(EventHeaders.EVENT_TYPE, event.eventType());
        headers.put(EventHeaders.CORRELATION_ID, correlationId);
        headers.put(EventHeaders.CONTENT_TYPE, "application/avro");
        headers.putAll(extraHeaders);
        return headers;
    }
}
