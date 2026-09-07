package fr.orderflow.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;

/**
 * Stock d'un produit.
 *
 * <p>Le {@code @Version} active le verrouillage optimiste : deux consumers
 * traitant simultanement deux commandes sur le meme produit ne peuvent pas
 * ecraser mutuellement leur reservation. Hibernate leve alors une
 * {@code OptimisticLockingFailureException}, que ton gestionnaire d'erreur
 * Kafka devra traiter comme une erreur RETRYABLE (contrairement a une erreur
 * de deserialisation).
 */
@Entity
@Table(name = "stock")
public class StockEntity {

    @Id
    @Column(name = "product_id", length = 64)
    private String productId;

    @Column(name = "quantity_available", nullable = false)
    private int quantityAvailable;

    @Column(name = "quantity_reserved", nullable = false)
    private int quantityReserved;

    @Version
    @Column(name = "version")
    private long version;

    protected StockEntity() {
        // requis par JPA
    }

    public StockEntity(String productId, int quantityAvailable) {
        this.productId = productId;
        this.quantityAvailable = quantityAvailable;
        this.quantityReserved = 0;
    }

    public boolean canReserve(int quantity) {
        return quantityAvailable >= quantity;
    }

    public void reserve(int quantity) {
        if (!canReserve(quantity)) {
            throw new IllegalStateException(
                    "Stock insuffisant pour " + productId + " : demande=" + quantity
                            + " disponible=" + quantityAvailable);
        }
        this.quantityAvailable -= quantity;
        this.quantityReserved += quantity;
    }

    /** Compensation : libere une reservation apres annulation de la commande. */
    public void release(int quantity) {
        int toRelease = Math.min(quantity, quantityReserved);
        this.quantityReserved -= toRelease;
        this.quantityAvailable += toRelease;
    }

    public String getProductId() {
        return productId;
    }

    public int getQuantityAvailable() {
        return quantityAvailable;
    }

    public int getQuantityReserved() {
        return quantityReserved;
    }
}
