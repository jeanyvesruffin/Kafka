package fr.orderflow.order.repository;

import fr.orderflow.order.domain.OutboxEventEntity;
import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEventEntity, String> {

    /**
     * Les evenements en attente, dans l'ordre d'ecriture.
     *
     * <p>Le {@link Limit} borne la taille du lot : on ne veut pas charger
     * 100 000 lignes si le broker a ete indisponible longtemps.
     */
    List<OutboxEventEntity> findByPublishedFalseOrderByCreatedAtAsc(Limit limit);

    long countByPublishedFalse();
}
