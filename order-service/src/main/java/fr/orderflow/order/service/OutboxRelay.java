package fr.orderflow.order.service;

import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventPublisher;
import fr.orderflow.order.domain.OutboxEventEntity;
import fr.orderflow.order.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Relais d'outbox : lit les evenements non publies et les pousse sur le bus.
 *
 * <p>Tant que tu n'as pas branche Kafka, {@link EventPublisher} est le
 * publisher de log : les evenements defilent dans la console et sont marques
 * publies. La chaine est donc observable de bout en bout sans broker.
 *
 * <p><b>Point important a comprendre.</b> Ce relais garantit une livraison
 * AT-LEAST-ONCE, pas exactly-once : si le service crashe entre l'envoi reussi
 * et le {@code markPublished}, l'evenement repartira au redemarrage. C'est
 * exactement pour cette raison que les consommateurs doivent etre idempotents
 * (voir {@code ProcessedEventEntity} cote Inventory et Payment).
 *
 * <p>Le polling est volontairement naif. Pistes d'amelioration une fois Kafka
 * en place : verrouillage pessimiste pour supporter plusieurs instances,
 * publication asynchrone avec callback, ou bascule vers du CDC (Debezium).
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);
    private static final int BATCH_SIZE = 100;

    private final OutboxEventRepository outboxRepository;
    private final EventPublisher eventPublisher;
    private final Clock clock;

    public OutboxRelay(OutboxEventRepository outboxRepository,
                       EventPublisher eventPublisher,
                       Clock clock) {
        this.outboxRepository = outboxRepository;
        this.eventPublisher = eventPublisher;
        this.clock = clock;
    }

    @Scheduled(fixedDelayString = "${orderflow.outbox.poll-interval-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEventEntity> pending =
                outboxRepository.findByPublishedFalseOrderByCreatedAtAsc(Limit.of(BATCH_SIZE));
        if (pending.isEmpty()) {
            return;
        }

        for (OutboxEventEntity row : pending) {
            try {
                eventPublisher.publish(toEnvelope(row));
                row.markPublished(clock.instant());
            } catch (RuntimeException e) {
                // On s'arrete au premier echec pour preserver l'ordre des
                // evenements d'une meme commande. Le lot repartira au tour suivant.
                log.warn(
                        "Publication echouee eventId={} topic={} : {}. Nouvel essai au prochain cycle.",
                        row.getEventId(), row.getTopic(), e.getMessage());
                break;
            }
        }
    }

    private EventEnvelope toEnvelope(OutboxEventEntity row) {
        Map<String, String> headers = new LinkedHashMap<>();
        headers.put(EventHeaders.EVENT_ID, row.getEventId());
        headers.put(EventHeaders.EVENT_TYPE, row.getEventType());
        headers.put(EventHeaders.CORRELATION_ID, row.getCorrelationId());
        headers.put(EventHeaders.CONTENT_TYPE, "application/json");
        return new EventEnvelope(row.getTopic(), row.getAggregateId(), row.getPayload(), headers);
    }
}
