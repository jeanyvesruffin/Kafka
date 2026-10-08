package fr.orderflow.order.messaging;

import fr.orderflow.common.kafka.DeadLetterTopics;
import fr.orderflow.common.kafka.KafkaErrorHandling;
import fr.orderflow.common.kafka.KafkaRetryProperties;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.order.service.OrderNotFoundException;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;

/**
 * Gestion des erreurs des consumers d'order-service (phase 4).
 *
 * <p>Un seul bean {@code CommonErrorHandler} par service : Boot l'applique tout seul a la fabrique de
 * conteneurs. S'il y en avait deux, Boot n'en appliquerait aucun (injection {@code getIfUnique}) et
 * les consumers retomberaient sans bruit sur le gestionnaire par defaut, qui jette le message.
 *
 * <p>{@link OrderNotFoundException} va directement en DLT : reessayer ne fera pas apparaitre une
 * commande qui n'existe pas (base reinitialisee alors que Kafka a conserve les messages).
 */
@Configuration
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
public class KafkaErrorHandlingOrderConfig {

    @Bean
    DefaultErrorHandler orderErrorHandler(
            KafkaOperations<?, ?> kafkaTemplate,
            KafkaRetryProperties retry,
            ObjectProvider<MeterRegistry> meterRegistry) {
        return KafkaErrorHandling.blockingRetry(
                kafkaTemplate, retry.blocking(), meterRegistry.getIfAvailable(), OrderNotFoundException.class);
    }

    @Bean
    KafkaAdmin.NewTopics orderDeadLetterTopics() {
        return DeadLetterTopics.of(
                Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED,
                Topics.PAYMENTS_COMPLETED, Topics.PAYMENTS_FAILED);
    }
}
