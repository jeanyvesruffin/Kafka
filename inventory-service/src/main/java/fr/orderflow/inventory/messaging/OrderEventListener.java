package fr.orderflow.inventory.messaging;

import fr.orderflow.common.event.OrderCancelledEvent;
import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.inventory.service.InventoryService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final InventoryService inventoryService;
    private final EventSerializer eventSerializer;

    @KafkaListener(topics = Topics.ORDERS_CREATED, groupId = "inventory-service")
    public void onOrderCreated(@Payload String payload,
                               @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        inventoryService.handleOrderCreated(
                eventSerializer.fromJson(payload, OrderCreatedEvent.class), correlationId);
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED, groupId = "inventory-service")
    public void onOrderCancelled(@Payload String payload,
                                 @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        inventoryService.handleOrderCancelled(
                eventSerializer.fromJson(payload, OrderCancelledEvent.class));
    }
}
