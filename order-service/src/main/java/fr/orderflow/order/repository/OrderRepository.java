package fr.orderflow.order.repository;

import fr.orderflow.order.domain.OrderEntity;
import fr.orderflow.order.domain.OrderStatus;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

/**
 * Les lectures chargent les lignes avec la commande ({@code @EntityGraph}) :
 * l'API construit sa reponse apres la fin de la transaction
 * ({@code spring.jpa.open-in-view: false}), un chargement paresseux des
 * {@code items} leverait alors une {@code LazyInitializationException}.
 */
public interface OrderRepository extends JpaRepository<OrderEntity, String> {

    @Override
    @EntityGraph(attributePaths = "items")
    Optional<OrderEntity> findById(String id);

    @Override
    @EntityGraph(attributePaths = "items")
    List<OrderEntity> findAll();

    @EntityGraph(attributePaths = "items")
    List<OrderEntity> findByStatus(OrderStatus status);

    List<OrderEntity> findByCustomerId(String customerId);
}
