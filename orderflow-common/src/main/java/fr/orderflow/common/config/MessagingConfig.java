package fr.orderflow.common.config;

import fr.orderflow.common.messaging.EventPublisher;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.LoggingEventPublisher;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

import java.time.Clock;

/**
 * Beans partages par tous les services.
 *
 * <h3>Comment basculer vers Kafka</h3>
 * Le publisher est choisi par la propriete {@code orderflow.messaging.publisher} :
 * <ul>
 *   <li>{@code logging} (valeur par defaut) — le squelette actuel, sans broker</li>
 *   <li>toute autre valeur — le bean {@link LoggingEventPublisher} n'est pas cree,
 *       c'est a toi de fournir un {@link EventPublisher}</li>
 * </ul>
 *
 * <p>Concretement, quand tu ecriras ton {@code KafkaEventPublisher}, il te
 * suffira de mettre {@code orderflow.messaging.publisher: kafka} dans
 * l'{@code application.yml} du service.
 *
 * <p><b>Pourquoi une propriete et pas simplement {@code @ConditionalOnMissingBean}
 * sur un {@code @Component} ?</b> Parce que la condition serait evaluee pendant
 * le scan de composants, dans un ordre non garanti par rapport a ta propre
 * classe. Selon le nom des packages, tu obtiendrais tantot un bean, tantot deux,
 * tantot une {@code NoUniqueBeanDefinitionException}. Le basculement par
 * propriete, lui, est deterministe. C'est aussi la raison pour laquelle
 * {@code @ConditionalOnMissingBean} est documente comme reserve aux classes
 * d'auto-configuration.
 */
@Configuration
public class MessagingConfig {

    @Bean
    @ConditionalOnProperty(
            name = "orderflow.messaging.publisher",
            havingValue = "logging",
            matchIfMissing = true)
    public EventPublisher loggingEventPublisher() {
        return new LoggingEventPublisher();
    }

    @Bean
    public EventSerializer eventSerializer(JsonMapper jsonMapper) {
        return new EventSerializer(jsonMapper);
    }

    /**
     * Horloge injectable partout, plutot que des appels a {@code Instant.now()}
     * en dur : les tests peuvent alors figer le temps avec {@code Clock.fixed(...)}.
     */
    @Bean
    @ConditionalOnMissingBean
    public Clock clock() {
        return Clock.systemUTC();
    }
}
