package fr.orderflow.common.event;

import java.math.BigDecimal;
import java.time.Instant;

public record PaymentCompletedEvent(
        String eventId,
        String orderId,
        BigDecimal amount,
        String transactionId,
        Instant occurredAt) implements OrderFlowEvent {

    public static final String TYPE = "PaymentCompleted";

    @Override
    public String eventType() {
        return TYPE;
    }
}
