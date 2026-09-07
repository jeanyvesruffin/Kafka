package fr.orderflow.inventory.repository;

import fr.orderflow.inventory.domain.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, String> {
}
