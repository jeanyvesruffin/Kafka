package fr.orderflow.order.service;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Catalogue de prix bouchonne.
 *
 * <p>Hors perimetre du cas d'ecole : on ne veut pas d'un service Catalogue
 * supplementaire. Un produit inconnu vaut 9.99.
 */
@Component
public class PriceCatalog {

    private static final BigDecimal DEFAULT_PRICE = new BigDecimal("9.99");

    private static final Map<String, BigDecimal> PRICES = Map.of(
            "sku-001", new BigDecimal("19.90"),
            "sku-002", new BigDecimal("49.00"),
            "sku-003", new BigDecimal("7.50"),
            "sku-004", new BigDecimal("299.00"),
            "sku-005", new BigDecimal("1250.00")
    );

    public BigDecimal priceOf(String productId) {
        return PRICES.getOrDefault(productId, DEFAULT_PRICE);
    }
}
