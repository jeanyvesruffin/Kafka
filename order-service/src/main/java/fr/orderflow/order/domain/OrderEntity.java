package fr.orderflow.order.domain;

import jakarta.persistence.*;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Commande. Lombok ne genere ici que les getters et le constructeur vide
 * exige par JPA : pas de {@code @Data}, {@code @EqualsAndHashCode} ni
 * {@code @ToString}, qui parcourraient la collection {@code items} (chargement
 * paresseux force, boucle infinie avec la reference inverse).
 */
@Entity
@Table(name = "orders")
@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class OrderEntity {

    @Id
    @Column(name = "id", length = 64)
    private String id;

    @Column(name = "customer_id", nullable = false, length = 64)
    private String customerId;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false, length = 32)
    private OrderStatus status;

    @Column(name = "total_amount", nullable = false, precision = 12, scale = 2)
    private BigDecimal totalAmount;

    @Column(name = "cancellation_reason", length = 255)
    private String cancellationReason;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    @OneToMany(mappedBy = "order", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<OrderItemEntity> items = new ArrayList<>();

    public OrderEntity(String id, String customerId, BigDecimal totalAmount, Instant now) {
        this.id = id;
        this.customerId = customerId;
        this.totalAmount = totalAmount;
        this.status = OrderStatus.CREATED;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public void addItem(OrderItemEntity item) {
        items.add(item);
        item.attachTo(this);
    }

    /**
     * Transition d'etat protegee : un etat terminal n'est jamais quitte.
     *
     * <p>C'est le premier niveau d'idempotence du service : rejouer deux fois
     * un {@code PaymentCompleted} ne change rien la seconde fois.
     *
     * @return true si la transition a effectivement eu lieu
     */
    public boolean transitionTo(OrderStatus target, String reason, Instant now) {
        if (status.isTerminal()) {
            return false;
        }
        if (status == target) {
            return false;
        }
        this.status = target;
        this.cancellationReason = reason;
        this.updatedAt = now;
        return true;
    }
}
