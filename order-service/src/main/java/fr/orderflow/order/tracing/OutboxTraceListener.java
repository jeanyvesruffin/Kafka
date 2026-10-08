package fr.orderflow.order.tracing;

import fr.orderflow.order.domain.OutboxEventEntity;
import jakarta.persistence.PrePersist;
import org.springframework.stereotype.Component;

/**
 * Listener JPA de {@link OutboxEventEntity} : memorise le contexte de trace courant dans la ligne au
 * moment ou elle est inseree (voir {@link OutboxTracing}).
 *
 * <p>Boot configure Hibernate avec un {@code SpringBeanContainer} : ce listener est donc un bean Spring
 * et peut recevoir ses dependances par injection. Passer par un listener evite de toucher
 * {@code OrderService}, qui n'a pas a savoir que la ligne d'outbox porte une trace.
 */
@Component
public class OutboxTraceListener {

    private final OutboxTracing tracing;

    public OutboxTraceListener(OutboxTracing tracing) {
        this.tracing = tracing;
    }

    @PrePersist
    void captureTraceParent(OutboxEventEntity row) {
        row.attachTraceParent(tracing.currentTraceParent());
    }
}
