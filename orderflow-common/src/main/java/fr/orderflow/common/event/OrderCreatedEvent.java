package fr.orderflow.common.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderCreatedEvent(
        String eventId,
        String orderId,
        String customerId,
        List<OrderLine> items,
        BigDecimal totalAmount,
        Instant occurredAt) implements OrderFlowEvent {

    public static final String TYPE = "OrderCreated";

    @Override
    public String eventType() {
        return TYPE;
    }
}
