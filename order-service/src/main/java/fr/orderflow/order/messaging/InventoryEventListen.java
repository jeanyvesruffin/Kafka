package fr.orderflow.order.messaging;


import fr.orderflow.common.event.InventoryRejectedEvent;
import fr.orderflow.common.event.InventoryReservedEvent;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.order.service.OrderService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


@Component
public class InventoryEventListen {

    private final OrderService orderService;

    private final EventSerializer eventSerializer;

    public InventoryEventListen(OrderService orderService, EventSerializer eventSerializer) {
        this.orderService = orderService;
        this.eventSerializer = eventSerializer;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "order-service")
    public void onReserved(@Payload String payload, @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        var event = eventSerializer.fromJson(payload, InventoryReservedEvent.class);
        orderService.onInventoryReserved(event.orderId(), correlationId);
    }

    @KafkaListener(topics = Topics.INVENTORY_REJECTED, groupId = "order-service")
    public void onReject(@Payload String payload, @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        var event = eventSerializer.fromJson(payload, InventoryRejectedEvent.class);
        orderService.onInventoryRejected(event.orderId(), event.reason(), correlationId);
    }

}
