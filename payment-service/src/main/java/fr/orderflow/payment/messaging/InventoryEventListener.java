package fr.orderflow.payment.messaging;

import fr.orderflow.common.event.InventoryReservedEvent;

import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.payment.service.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class InventoryEventListener {

    private final PaymentService paymentService;
    private final EventSerializer eventSerializer;

    public InventoryEventListener(PaymentService paymentService, EventSerializer eventSerializer) {
        this.paymentService = paymentService;
        this.eventSerializer = eventSerializer;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "payment-service")
    public void onInventoryReserved(@Payload String payload, @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        paymentService.handleInventoryReserved(
                eventSerializer.fromJson(payload, InventoryReservedEvent.class),
                correlationId);
    }

}
