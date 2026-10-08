package fr.orderflow.notification.messaging;

import fr.orderflow.common.kafka.DeadLetterTopics;
import fr.orderflow.common.kafka.KafkaErrorHandling;
import fr.orderflow.common.kafka.KafkaRetryProperties;
import fr.orderflow.common.messaging.Topics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * Gestion des erreurs du consumer de notification-service (phase 4).
 *
 * <p>Un evenement de type inattendu ({@link IllegalArgumentException} du listener) ou un JSON illisible
 * part directement en DLT. Une erreur transitoire est rejouee avec un backoff exponentiel avant d'y
 * partir a son tour.
 */
@Configuration
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
public class KafkaErrorHandlingNotificationConfig {

    @Bean
    DefaultErrorHandler notificationErrorHandler(
            KafkaOperations<?, ?> kafkaTemplate,
            KafkaRetryProperties retry,
            ObjectProvider<MeterRegistry> meterRegistry) {
        return KafkaErrorHandling.blockingRetry(kafkaTemplate, retry.blocking(), meterRegistry.getIfAvailable());
    }

    @Bean
    KafkaAdmin.NewTopics notificationDeadLetterTopics() {
        return DeadLetterTopics.of(Topics.ORDERS_CONFIRMED, Topics.ORDERS_CANCELLED);
    }
}
