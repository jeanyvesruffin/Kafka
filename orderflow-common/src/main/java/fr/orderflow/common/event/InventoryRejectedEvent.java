package fr.orderflow.common.event;

import java.time.Instant;

public record InventoryRejectedEvent(
        String eventId,
        String orderId,
        String reason,
        Instant occurredAt) implements OrderFlowEvent {

    public static final String TYPE = "InventoryRejected";

    @Override
    public String eventType() {
        return TYPE;
    }
}
