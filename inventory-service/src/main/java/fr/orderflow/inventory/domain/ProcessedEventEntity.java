package fr.orderflow.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;

/**
 * Journal des evenements deja traites — pattern Idempotent Consumer.
 *
 * <p>La cle primaire est l'{@code eventId}. L'insertion se fait dans la meme
 * transaction que le traitement metier : soit les deux passent, soit aucun.
 * Un evenement redelivre (rebalance, crash, redemarrage) trouve sa ligne deja
 * presente et est ignore sans effet de bord.
 *
 * <p>En production, cette table se purge (les evenements de plus de N jours ne
 * peuvent plus etre redelivres compte tenu de la retention des topics).
 */
@Entity
@Table(name = "processed_events")
@Getter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class ProcessedEventEntity {

    @Id
    @Column(name = "event_id", length = 64)
    private String eventId;

    @Column(name = "event_type", nullable = false, length = 64)
    private String eventType;

    @Column(name = "processed_at", nullable = false)
    private Instant processedAt;
}
