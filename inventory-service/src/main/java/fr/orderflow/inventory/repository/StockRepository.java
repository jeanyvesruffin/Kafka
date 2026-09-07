package fr.orderflow.inventory.repository;

import fr.orderflow.inventory.domain.StockEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface StockRepository extends JpaRepository<StockEntity, String> {
}
