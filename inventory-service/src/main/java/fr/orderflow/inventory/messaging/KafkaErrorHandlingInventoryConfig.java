package fr.orderflow.inventory.messaging;

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
 * Gestion des erreurs des consumers d'inventory-service (phase 4).
 *
 * <p><b>Pourquoi des reprises bloquantes ici ?</b> {@code StockEntity} porte un {@code @Version} : deux
 * commandes concurrentes sur le meme produit peuvent lever une
 * {@code OptimisticLockingFailureException}. L'erreur se resorbe d'elle-meme : au second essai, la
 * transaction relit le stock a jour et passe. Classee <b>retryable</b> (c'est le comportement par
 * defaut : seules les erreurs listees dans {@link KafkaErrorHandling} sont directement envoyees en
 * DLT), elle est rejouee apres 200 ms, 400 ms, 800 ms... puis envoyee en DLT si le verrou ne se
 * libere pas.
 *
 * <p>Les reprises <i>non</i> bloquantes (topics {@code -retry-N}) seraient ici dangereuses : elles
 * font perdre l'ordre. Un {@code orders.created} rejoue apres un {@code orders.cancelled} de la meme
 * commande reserverait un stock que plus personne ne liberera. Le consumer bloque donc la partition
 * quelques centaines de millisecondes, ce qui preserve l'ordre.
 *
 * <p>Une {@code DeserializationException} ou un JSON illisible n'est jamais rejoue : DLT immediat.
 */
@Configuration
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
public class KafkaErrorHandlingInventoryConfig {

    @Bean
    DefaultErrorHandler inventoryErrorHandler(
            KafkaOperations<?, ?> kafkaTemplate,
            KafkaRetryProperties retry,
            ObjectProvider<MeterRegistry> meterRegistry) {
        return KafkaErrorHandling.blockingRetry(kafkaTemplate, retry.blocking(), meterRegistry.getIfAvailable());
    }

    @Bean
    KafkaAdmin.NewTopics inventoryDeadLetterTopics() {
        return DeadLetterTopics.of(Topics.ORDERS_CREATED, Topics.ORDERS_CANCELLED);
    }
}
