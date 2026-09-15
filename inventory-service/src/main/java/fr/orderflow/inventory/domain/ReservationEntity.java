package fr.orderflow.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * Quantite de stock reservee pour une commande donnee.
 *
 * <p>C'est la source de verite de la compensation : a l'annulation d'une
 * commande, on ne libere que ce qui a ete reserve pour ELLE. Le stock agrege
 * ({@link StockEntity#getQuantityReserved()}) ne suffit pas : une commande
 * rejetee faute de stock n'a rien reserve, et liberer ses quantites viderait
 * les reservations des autres commandes en cours sur le meme produit.
 */
@Entity
@Table(name = "stock_reservation")
@Getter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ReservationEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "order_id", nullable = false, length = 64)
    private String orderId;

    @Column(name = "product_id", nullable = false, length = 64)
    private String productId;

    @Column(name = "quantity", nullable = false)
    private int quantity;
}
