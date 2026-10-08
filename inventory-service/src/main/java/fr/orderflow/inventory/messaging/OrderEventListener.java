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

    /**
     * {@code orders.created} est en <b>Avro</b> (phase 7) : ce listener recoit directement l'evenement
     * type, decode par {@link fr.orderflow.common.kafka.OrderCreatedAvroDeserializer}.
     *
     * <p>Les {@code properties} remplacent, pour ce seul listener, le deserialiseur de valeur de
     * l'{@code application.yml} (String). Le deserialiseur reste enveloppe dans un
     * {@code ErrorHandlingDeserializer} : un message illisible (JSON de l'ancien format, octets
     * corrompus, schema inconnu) ne bloque pas le consumer, il est envoye sur {@code orders.created.DLT}
     * avec ses octets d'origine.
     */
    @KafkaListener(topics = Topics.ORDERS_CREATED, groupId = "inventory-service",
            properties = {
                    "value.deserializer=org.springframework.kafka.support.serializer.ErrorHandlingDeserializer",
                    "spring.deserializer.value.delegate.class=fr.orderflow.common.kafka.OrderCreatedAvroDeserializer"})
    public void onOrderCreated(@Payload OrderCreatedEvent event,
                               @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        inventoryService.handleOrderCreated(event, correlationId);
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED, groupId = "inventory-service")
    public void onOrderCancelled(@Payload String payload,
                                 @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        inventoryService.handleOrderCancelled(
                eventSerializer.fromJson(payload, OrderCancelledEvent.class));
    }
}
