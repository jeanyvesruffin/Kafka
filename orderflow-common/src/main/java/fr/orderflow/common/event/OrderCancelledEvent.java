package fr.orderflow.common.event;

import java.time.Instant;
import java.util.List;

public record OrderCancelledEvent(
        String eventId,
        String orderId,
        List<OrderLine> items,
        String reason,
        Instant occurredAt) implements OrderFlowEvent {

    public static final String TYPE = "OrderCancelled";

    @Override
    public String eventType() {
        return TYPE;
    }
}
