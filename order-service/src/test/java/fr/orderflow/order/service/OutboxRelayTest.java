package fr.orderflow.order.service;

import fr.orderflow.common.messaging.CorrelationId;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.order.domain.OutboxEventEntity;
import fr.orderflow.order.repository.OutboxEventRepository;
import fr.orderflow.order.tracing.OutboxTracing;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.MDC;
import org.springframework.data.domain.Limit;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Le relais d'outbox tourne hors de toute requete : verifie qu'il donne a ses logs le
 * {@code correlationId} de la ligne qu'il publie (phase 5).
 */
class OutboxRelayTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:15:30Z");

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    private static OutboxEventEntity row(String eventId, String correlationId) {
        return new OutboxEventEntity(
                "out-" + eventId, "ord-1", eventId, "OrderCreated", "orders.created", "{}", correlationId, NOW);
    }

    private static OutboxEventRepository repositoryReturning(List<OutboxEventEntity> rows) {
        OutboxEventRepository repository = Mockito.mock(OutboxEventRepository.class);
        Mockito.when(repository.findByPublishedFalseOrderByCreatedAtAsc(Mockito.any(Limit.class)))
                .thenReturn(rows);
        return repository;
    }

    @Test
    @DisplayName("Chaque ligne est publiee avec SON correlationId dans le MDC, retire ensuite")
    void eachRow_isPublishedWithItsOwnCorrelationId() {
        List<String> mdcSeenByPublisher = new ArrayList<>();
        List<String> headerSeenByPublisher = new ArrayList<>();
        OutboxRelay relay = new OutboxRelay(
                repositoryReturning(List.of(row("e1", "corr-A"), row("e2", "corr-B"))),
                (EventEnvelope envelope) -> {
                    mdcSeenByPublisher.add(MDC.get(CorrelationId.MDC_KEY));
                    headerSeenByPublisher.add(envelope.headers().get(EventHeaders.CORRELATION_ID));
                },
                Clock.fixed(NOW, ZoneOffset.UTC),
                OutboxTracing.disabled());

        relay.publishPending();

        assertThat(mdcSeenByPublisher).containsExactly("corr-A", "corr-B");
        assertThat(headerSeenByPublisher).containsExactly("corr-A", "corr-B");
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @Test
    @DisplayName("Echec de publication : le MDC est nettoye quand meme et les lignes suivantes ne sont pas tentees")
    void failedPublication_clearsMdcAndStops() {
        List<OutboxEventEntity> rows = List.of(row("e1", "corr-A"), row("e2", "corr-B"));
        List<String> published = new ArrayList<>();
        OutboxRelay relay = new OutboxRelay(
                repositoryReturning(rows),
                envelope -> {
                    published.add(envelope.headers().get(EventHeaders.EVENT_ID));
                    throw new IllegalStateException("broker indisponible");
                },
                Clock.fixed(NOW, ZoneOffset.UTC),
                OutboxTracing.disabled());

        relay.publishPending();

        assertThat(published).containsExactly("e1");        // arret au premier echec : l'ordre est preserve
        assertThat(rows).extracting(OutboxEventEntity::isPublished).containsExactly(false, false);
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }
}
