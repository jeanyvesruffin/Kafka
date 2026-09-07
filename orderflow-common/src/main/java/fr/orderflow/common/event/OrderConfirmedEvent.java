package fr.orderflow.common.event;

import java.time.Instant;

public record OrderConfirmedEvent(
        String eventId,
        String orderId,
        Instant occurredAt) implements OrderFlowEvent {

    public static final String TYPE = "OrderConfirmed";

    @Override
    public String eventType() {
        return TYPE;
    }
}
