package fr.orderflow.common.messaging;

import lombok.extern.slf4j.Slf4j;

/**
 * Implementation par defaut : n'envoie rien, se contente de tracer.
 *
 * <p>Permet de faire tourner et de tester toute la chaine metier sans broker.
 * Le message logue contient exactement ce que Kafka transportera : topic, cle
 * de partition, en-tetes, payload.
 *
 * <p>Ce n'est pas un {@code @Component} : le bean est declare explicitement
 * dans {@link fr.orderflow.common.config.MessagingConfig}, ce qui rend le
 * basculement vers Kafka deterministe.
 */
@Slf4j
public class LoggingEventPublisher implements EventPublisher {

    @Override
    public void publish(EventEnvelope envelope) {
        log.info(
                "[NO-BROKER] topic={} key={} headers={} payload={}",
                envelope.topic(), envelope.key(), envelope.headers(), envelope.payload());
    }
}
