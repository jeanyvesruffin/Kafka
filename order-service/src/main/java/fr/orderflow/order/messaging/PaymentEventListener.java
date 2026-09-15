package fr.orderflow.order.messaging;

import fr.orderflow.common.event.PaymentCompletedEvent;
import fr.orderflow.common.event.PaymentFailedEvent;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.order.service.OrderService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


@Component
@RequiredArgsConstructor
public class PaymentEventListener {

    private final OrderService orderService;
    private final EventSerializer eventSerializer;

    @KafkaListener(topics = Topics.PAYMENTS_COMPLETED, groupId = "order-service")
    public void onCompleted(@Payload String payload, @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        var event = eventSerializer.fromJson(payload, PaymentCompletedEvent.class);
        orderService.onPaymentCompleted(event.orderId(), correlationId);
    }

    @KafkaListener(topics = Topics.PAYMENTS_FAILED, groupId = "order-service")
    public void onFailed(@Payload String payload, @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        var event = eventSerializer.fromJson(payload, PaymentFailedEvent.class);
        orderService.onPaymentFailed(event.orderId(), event.reason(), correlationId);
    }
}
