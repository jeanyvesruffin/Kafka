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

    public static final String ORDERS_CREATED = "orders.created";
    public static final String ORDERS_CANCELLED = "orders.cancelled";
    public static final String ORDERS_CONFIRMED = "orders.confirmed";
    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_REJECTED = "inventory.rejected";
    public static final String PAYMENTS_COMPLETED = "payments.completed";
    public static final String PAYMENTS_FAILED = "payments.failed";

    /**
     * Suffixe des Dead Letter Topics : {@code <topic>.DLT} (phase 4).
     */
    public static final String DLT_SUFFIX = ".DLT";

    private Topics() {
    }

    /**
     * Topics dont le contenu est en Avro sur le fil (phase 7). Les autres restent en JSON.
     *
     * <p>Le format du fil est une affaire de transport : l'outbox et les services metier manipulent
     * toujours l'evenement (JSON dans l'outbox) ; la conversion se fait dans le publisher Kafka, et a
     * l'autre bout dans le deserialiseur du consumer.
     */
    public static boolean isAvro(String topic) {
        return ORDERS_CREATED.equals(topic);
    }

    /**
     * Dead Letter Topic d'un topic source : {@code orders.created} -> {@code orders.created.DLT}.
     */
    public static String dltOf(String topic) {
        return topic + DLT_SUFFIX;
    }
}
