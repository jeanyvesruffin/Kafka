package fr.orderflow.common.messaging;

import fr.orderflow.common.event.OrderFlowEvent;
import lombok.RequiredArgsConstructor;
import tools.jackson.databind.json.JsonMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Serialisation des evenements et construction des enveloppes.
 *
 * <p><b>Note Spring Boot 4 / Jackson 3.</b> Le bean auto-configure n'est plus
 * {@code com.fasterxml.jackson.databind.ObjectMapper} mais
 * {@code tools.jackson.databind.json.JsonMapper}, immuable et thread-safe.
 * Les exceptions Jackson 3 sont non checkees : plus de try/catch obligatoire.
 * Les types {@code java.time} (dont {@code Instant}) sont serialises en
 * ISO-8601 nativement, sans module a enregistrer.
 *
 * <p>Bean declare dans {@link fr.orderflow.common.config.MessagingConfig}.
 */
@RequiredArgsConstructor
public class EventSerializer {

    private final JsonMapper jsonMapper;

    public String toJson(OrderFlowEvent event) {
        return jsonMapper.writeValueAsString(event);
    }

    public <T extends OrderFlowEvent> T fromJson(String json, Class<T> type) {
        return jsonMapper.readValue(json, type);
    }

    /**
     * En-tetes standards d'un evenement, correlationId compris.
     */
    public Map<String, String> headersFor(OrderFlowEvent event, String correlationId) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(EventHeaders.EVENT_ID, event.eventId());
        headers.put(EventHeaders.EVENT_TYPE, event.eventType());
        headers.put(EventHeaders.CORRELATION_ID, correlationId);
        headers.put(EventHeaders.CONTENT_TYPE, "application/json");
        return headers;
    }

    /**
     * Raccourci : evenement -> enveloppe prete a publier.
     */
    public EventEnvelope envelope(String topic, OrderFlowEvent event, String correlationId) {
        return new EventEnvelope(topic, event.orderId(), toJson(event), headersFor(event, correlationId));
    }
}
