package fr.orderflow.common.messaging;

/**
 * Cles des en-tetes transportes avec chaque message.
 */
public final class EventHeaders {

    public static final String EVENT_ID = "eventId";
    public static final String EVENT_TYPE = "eventType";
    public static final String CORRELATION_ID = "correlationId";
    public static final String CONTENT_TYPE = "contentType";

    private EventHeaders() {
    }
}
