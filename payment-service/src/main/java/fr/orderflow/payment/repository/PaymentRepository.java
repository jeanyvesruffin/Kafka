package fr.orderflow.payment.repository;

import fr.orderflow.payment.domain.PaymentEntity;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface PaymentRepository extends JpaRepository<PaymentEntity, String> {

    List<PaymentEntity> findByOrderId(String orderId);
}
