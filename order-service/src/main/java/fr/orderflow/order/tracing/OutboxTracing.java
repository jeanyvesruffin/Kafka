package fr.orderflow.order.tracing;

import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.springframework.lang.Nullable;

import java.util.HashMap;
import java.util.Map;

/**
 * Garde la trace distribuee continue a travers l'outbox (phase 5).
 *
 * <p><b>Le probleme.</b> L'outbox decouple volontairement l'ecriture de la commande (dans la requete
 * HTTP) de la publication Kafka (plus tard, par le relais, sur un autre thread). Sans precaution, le
 * relais demarre une trace neuve : la trace de la requete s'arrete a l'ecriture en base, et tout ce qui
 * suit (inventory, payment...) appartient a une autre trace.
 *
 * <p><b>La solution.</b> A l'insertion de la ligne d'outbox, on note le contexte de trace courant au
 * format W3C {@code traceparent} dans la ligne ({@link #currentTraceParent()}). A la publication, le relais
 * ouvre un span enfant de ce contexte ({@link #runInSpan}) : l'instrumentation du {@code KafkaTemplate}
 * y rattache le span d'envoi et ecrit le {@code traceparent} dans l'en-tete du message, que les
 * consumers reprennent.
 *
 * <p>Sans tracing sur le classpath ou desactive, tout est inerte ({@link #disabled()}).
 */
public class OutboxTracing {

    static final String TRACEPARENT = "traceparent";

    @Nullable
    private final Tracer tracer;
    @Nullable
    private final Propagator propagator;

    public OutboxTracing(@Nullable Tracer tracer, @Nullable Propagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    public static OutboxTracing disabled() {
        return new OutboxTracing(null, null);
    }

    /**
     * Le {@code traceparent} du span courant, ou null s'il n'y a pas de trace en cours.
     */
    @Nullable
    public String currentTraceParent() {
        if (tracer == null || propagator == null) {
            return null;
        }
        TraceContext context = tracer.currentTraceContext().context();
        if (context == null) {
            return null;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(context, carrier, Map::put);
        return carrier.get(TRACEPARENT);
    }

    /**
     * Execute l'action dans un span enfant du {@code traceparent} donne. Sans contexte memorise
     * (ligne ecrite avant la phase 5, tracing inactif), l'action s'execute telle quelle.
     */
    public void runInSpan(@Nullable String traceParent, String spanName, Runnable action) {
        if (tracer == null || propagator == null || traceParent == null) {
            action.run();
            return;
        }
        Span span = propagator.extract(Map.of(TRACEPARENT, traceParent), Map::get)
                .name(spanName)
                .start();
        try (Tracer.SpanInScope ignored = tracer.withSpan(span)) {
            action.run();
        } catch (RuntimeException e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }
}
