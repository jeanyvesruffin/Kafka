package fr.orderflow.inventory.repository;

import fr.orderflow.inventory.domain.ReservationEntity;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ReservationRepository extends JpaRepository<ReservationEntity, String> {

    List<ReservationEntity> findByOrderId(String orderId);
}
