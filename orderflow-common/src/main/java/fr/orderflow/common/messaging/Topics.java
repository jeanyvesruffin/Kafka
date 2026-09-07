package fr.orderflow.common.messaging;

/**
 * Noms logiques des topics.
 *
 * <p>Ces constantes sont utilisees des maintenant par le squelette (le
 * publisher de log les affiche). Quand tu brancheras Kafka, ce sont ces
 * memes constantes que tu passeras a {@code KafkaTemplate.send(...)} et a
 * {@code @KafkaListener(topics = ...)} : aucune chaine en dur ailleurs
 * dans le code.
 */
public final class Topics {

    public static final String ORDERS_CREATED       = "orders.created";
    public static final String ORDERS_CANCELLED     = "orders.cancelled";
    public static final String ORDERS_CONFIRMED     = "orders.confirmed";
    public static final String INVENTORY_RESERVED   = "inventory.reserved";
    public static final String INVENTORY_REJECTED   = "inventory.rejected";
    public static final String PAYMENTS_COMPLETED   = "payments.completed";
    public static final String PAYMENTS_FAILED      = "payments.failed";

    private Topics() {
    }
}
