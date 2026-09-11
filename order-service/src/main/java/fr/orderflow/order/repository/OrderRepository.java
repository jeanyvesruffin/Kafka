package fr.orderflow.order.repository;

import fr.orderflow.order.domain.OrderEntity;
import fr.orderflow.order.domain.OrderStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OrderRepository extends JpaRepository<OrderEntity, String> {

    List<OrderEntity> findByStatus(OrderStatus status);

    List<OrderEntity> findByCustomerId(String customerId);
}
