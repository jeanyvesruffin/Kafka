package fr.orderflow.order.tracing;

import io.micrometer.tracing.propagation.Propagator;
import io.micrometer.tracing.Tracer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OutboxTracingConfig {

    /**
     * {@code ObjectProvider} : sans tracing actif (propriete {@code management.tracing.enabled=false},
     * ou bean absent), le service demarre quand meme avec un {@link OutboxTracing} inerte.
     */
    @Bean
    OutboxTracing outboxTracing(ObjectProvider<Tracer> tracer, ObjectProvider<Propagator> propagator) {
        return new OutboxTracing(tracer.getIfAvailable(), propagator.getIfAvailable());
    }
}
