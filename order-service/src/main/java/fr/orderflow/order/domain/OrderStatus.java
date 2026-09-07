package fr.orderflow.order.domain;

/**
 * Cycle de vie de la commande.
 *
 * <pre>
 *   CREATED --&gt; INVENTORY_RESERVED --&gt; CONFIRMED   (etat terminal)
 *      |                 |
 *      +-----------------+-----------&gt; CANCELLED   (etat terminal)
 * </pre>
 */
public enum OrderStatus {

    CREATED,
    INVENTORY_RESERVED,
    CONFIRMED,
    CANCELLED;

    public boolean isTerminal() {
        return this == CONFIRMED || this == CANCELLED;
    }
}
