package fr.orderflow.common.event;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

/**
 * Une commande vient d'etre creee (topic {@code orders.created}).
 *
 * <p>Seul evenement transporte en <b>Avro</b> sur le fil (phase 7) : voir
 * {@code fr.orderflow.common.messaging.avro.OrderCreatedAvroCodec} et les schemas
 * {@code src/main/avro/OrderCreated.avsc} (version courante) et
 * {@code avro/history/OrderCreated.v1.avsc} (version 1).
 *
 * @param couponCode code promotionnel saisi par le client, <b>optionnel</b> (null si absent). Champ
 *                   ajoute en version 2 du contrat, sans casser les consommateurs de la version 1.
 */
public record OrderCreatedEvent(
        String eventId,
        String orderId,
        String customerId,
        List<OrderLine> items,
        BigDecimal totalAmount,
        Instant occurredAt,
        String couponCode) implements OrderFlowEvent {

    public static final String TYPE = "OrderCreated";

    /**
     * Forme de la version 1 du contrat, sans coupon.
     */
    public OrderCreatedEvent(
            String eventId,
            String orderId,
            String customerId,
            List<OrderLine> items,
            BigDecimal totalAmount,
            Instant occurredAt) {
        this(eventId, orderId, customerId, items, totalAmount, occurredAt, null);
    }

    @Override
    public String eventType() {
        return TYPE;
    }
}
