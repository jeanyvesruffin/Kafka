package fr.orderflow.common.event;

import java.math.BigDecimal;

/**
 * Ligne de commande transportee dans les evenements.
 */
public record OrderLine(String productId, int quantity, BigDecimal unitPrice) {

    public BigDecimal lineTotal() {
        return unitPrice.multiply(BigDecimal.valueOf(quantity));
    }
}
