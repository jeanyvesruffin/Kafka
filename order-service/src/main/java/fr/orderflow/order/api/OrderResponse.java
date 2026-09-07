package fr.orderflow.order.api;

import fr.orderflow.order.domain.OrderEntity;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

public record OrderResponse(
        String orderId,
        String customerId,
        String status,
        BigDecimal totalAmount,
        String cancellationReason,
        List<Line> items,
        Instant createdAt,
        Instant updatedAt) {

    public record Line(String productId, int quantity, BigDecimal unitPrice, BigDecimal lineTotal) {
    }

    public static OrderResponse from(OrderEntity order) {
        return new OrderResponse(
                order.getId(),
                order.getCustomerId(),
                order.getStatus().name(),
                order.getTotalAmount(),
                order.getCancellationReason(),
                order.getItems().stream()
                        .map(i -> new Line(i.getProductId(), i.getQuantity(), i.getUnitPrice(), i.lineTotal()))
                        .toList(),
                order.getCreatedAt(),
                order.getUpdatedAt());
    }
}
