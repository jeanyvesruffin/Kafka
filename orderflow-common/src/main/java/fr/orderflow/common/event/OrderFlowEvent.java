package fr.orderflow.common.event;

import java.time.Instant;

/**
 * Racine scellee de tous les evenements metier d'OrderFlow.
 *
 * <p>Le fait que l'interface soit {@code sealed} permet un {@code switch}
 * exhaustif avec pattern matching cote consommateur, sans branche
 * {@code default} : le compilateur verifie que tous les cas sont traites.
 *
 * <pre>{@code
 * String describe(OrderFlowEvent e) {
 *     return switch (e) {
 *         case OrderCreatedEvent c      -> "commande " + c.orderId() + " creee";
 *         case InventoryReservedEvent r -> "stock reserve";
 *         // ... le compilateur exige toutes les branches
 *     };
 * }
 * }</pre>
 */
public sealed interface OrderFlowEvent
        permits OrderCreatedEvent, OrderCancelledEvent, OrderConfirmedEvent,
        InventoryReservedEvent, InventoryRejectedEvent,
        PaymentCompletedEvent, PaymentFailedEvent {

    /**
     * Identifiant unique de l'evenement. Cle de l'idempotence cote consommateur.
     */
    String eventId();

    /**
     * Identifiant de la commande. Sert de cle de partition Kafka.
     */
    String orderId();

    /**
     * Date de survenue metier (pas la date de publication).
     */
    Instant occurredAt();

    /**
     * Nom logique, place dans l'en-tete Kafka {@code eventType}.
     */
    String eventType();
}
