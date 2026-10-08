package fr.orderflow.payment.messaging;

import fr.orderflow.common.kafka.DeadLetterTopics;
import fr.orderflow.common.kafka.KafkaErrorHandling;
import fr.orderflow.common.kafka.KafkaRetryProperties;
import fr.orderflow.common.messaging.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaRetryTopic;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.retrytopic.RetryTopicConfiguration;
import org.springframework.kafka.retrytopic.RetryTopicConfigurationBuilder;
import org.springframework.kafka.retrytopic.RetryTopicSchedulerWrapper;
import org.springframework.kafka.support.EndpointHandlerMethod;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import tools.jackson.core.JacksonException;

/**
 * Reprises <b>non bloquantes</b> de payment-service (phase 4).
 *
 * <p>Un vrai encaissement appelle un tiers, qui echoue de facon transitoire (timeout reseau, 503) et
 * peut mettre plusieurs secondes, voire minutes, a revenir. Bloquer la partition pendant ce temps
 * arreterait le paiement de <b>toutes</b> les commandes qui s'y trouvent. Le message en echec est donc
 * republie sur un topic de retry et la partition continue d'avancer :
 *
 * <pre>
 * inventory.reserved --echec--> inventory.reserved-retry-0 (apres 1 s)
 *                    --echec--> inventory.reserved-retry-1 (apres 2 s)
 *                    --echec--> inventory.reserved.DLT     (+ {@link PaymentDeadLetterHandler})
 * </pre>
 *
 * <p>Compromis : un message rejoue arrive <i>apres</i> ceux qui le suivaient sur la partition. Sans
 * consequence ici (payment ne consomme qu'un topic, et le traitement est idempotent par
 * {@code eventId}) ; c'est ce qui l'exclurait pour inventory-service, voir
 * {@code KafkaErrorHandlingInventoryConfig}.
 *
 * <p>Erreurs <b>non retryables</b> (JSON illisible, {@code DeserializationException}...) : DLT
 * immediat, sans passer par les topics de retry.
 *
 * <p>Le {@link DefaultErrorHandler} ci-dessous couvre les listeners qui ne seraient pas concernes par
 * ces topics de retry ; sans lui, Boot appliquerait son gestionnaire par defaut, qui jette le message.
 */
@Configuration
@EnableKafkaRetryTopic
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
public class KafkaRetryTopicPaymentConfig {

    /**
     * Les topics de retry mettent les partitions en pause jusqu'a l'echeance du message ; ce
     * planificateur les reveille. Spring Kafka refuse de demarrer sans lui ("Either a
     * RetryTopicSchedulerWrapper or TaskScheduler bean is required") et Boot ne fournit un
     * {@code TaskScheduler} que si {@code @EnableScheduling} est present, ce qui n'est pas le cas ici.
     * Le wrapper l'initialise et l'arrete avec le contexte.
     */
    @Bean
    RetryTopicSchedulerWrapper retryTopicScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadNamePrefix("retry-topic-");
        return new RetryTopicSchedulerWrapper(scheduler);
    }

    @Bean
    RetryTopicConfiguration inventoryReservedRetryTopics(
            KafkaOperations<?, ?> kafkaTemplate, KafkaRetryProperties retry) {
        KafkaRetryProperties.Topics topics = retry.topics();
        return RetryTopicConfigurationBuilder.newInstance()
                .includeTopic(Topics.INVENTORY_RESERVED)
                .maxAttempts(topics.maxAttempts())
                .exponentialBackoff(
                        topics.initialInterval().toMillis(),
                        topics.multiplier(),
                        topics.maxInterval().toMillis())
                .suffixTopicsWithIndexValues()          // inventory.reserved-retry-0, -retry-1
                .dltSuffix(Topics.DLT_SUFFIX)           // inventory.reserved.DLT
                .autoCreateTopicsWith(DeadLetterTopics.PARTITIONS, DeadLetterTopics.REPLICAS)
                .notRetryOn(DeserializationException.class)
                .notRetryOn(MessageConversionException.class)
                .notRetryOn(JacksonException.class)
                .notRetryOn(IllegalArgumentException.class)
                .traversingCauses()                     // l'exception du listener enveloppe la vraie cause
                .dltHandlerMethod(new EndpointHandlerMethod(PaymentDeadLetterHandler.class, "onDeadLetter"))
                .create(kafkaTemplate);
    }

    @Bean
    DefaultErrorHandler paymentErrorHandler(
            KafkaOperations<?, ?> kafkaTemplate,
            KafkaRetryProperties retry,
            ObjectProvider<MeterRegistry> meterRegistry) {
        return KafkaErrorHandling.blockingRetry(kafkaTemplate, retry.blocking(), meterRegistry.getIfAvailable());
    }
}
