package fr.orderflow.notification.messaging;


import fr.orderflow.common.event.*;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.notification.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


/**
 * Consomme les evenements terminaux de la commande ({@code orders.confirmed},
 * {@code orders.cancelled}) et delegue la notification.
 */
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final NotificationService notificationService;

    private final EventSerializer eventSerializer;

    @KafkaListener(topics = { Topics.ORDERS_CONFIRMED, Topics.ORDERS_CANCELLED },
            groupId = "notification-service")
    public void onTerminalEvent(@Payload String payload,
                                @Header(EventHeaders.EVENT_TYPE) String eventType,
                                @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        OrderFlowEvent event = switch (eventType) {
            case OrderConfirmedEvent.TYPE -> eventSerializer.fromJson(payload, OrderConfirmedEvent.class);
            case OrderCancelledEvent.TYPE -> eventSerializer.fromJson(payload, OrderCancelledEvent.class);
            default -> throw new IllegalArgumentException("Type inattendu : " + eventType);
        };
        notificationService.notifyCustomer(event, correlationId);
    }

}
