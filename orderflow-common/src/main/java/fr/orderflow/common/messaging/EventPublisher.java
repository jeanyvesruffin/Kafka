package fr.orderflow.common.messaging;

/**
 * Port de sortie : publier un evenement sur le bus.
 *
 * <p>C'est le SEUL point d'extension que Kafka devra implementer cote
 * production. Le squelette fournit {@link LoggingEventPublisher}, qui se
 * contente de tracer : l'application tourne donc de bout en bout sans
 * broker.
 *
 * <p><b>Ce que tu auras a faire :</b> creer une classe
 * {@code KafkaEventPublisher implements EventPublisher} annotee
 * {@code @Component}. Des qu'elle existe, elle prend automatiquement la place
 * du publisher de log (celui-ci est declare {@code @ConditionalOnMissingBean}).
 */
public interface EventPublisher {

    void publish(EventEnvelope envelope);
}
