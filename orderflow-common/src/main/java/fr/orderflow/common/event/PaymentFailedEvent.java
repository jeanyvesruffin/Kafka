package fr.orderflow.common.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentFailedEvent(
        String eventId,
        String orderId,
        BigDecimal amount,
        String reason,
        Instant occurredAt) implements OrderFlowEvent {

    public static final String TYPE = "PaymentFailed";

    @Override
    public String eventType() {
        return TYPE;
    }
}
