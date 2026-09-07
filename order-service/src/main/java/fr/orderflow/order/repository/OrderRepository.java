package fr.orderflow.order.repository;

import fr.orderflow.order.domain.OrderEntity;
import fr.orderflow.order.domain.OrderStatus;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface OrderRepository extends JpaRepository<OrderEntity, String> {

    List<OrderEntity> findByStatus(OrderStatus status);

    List<OrderEntity> findByCustomerId(String customerId);
}
