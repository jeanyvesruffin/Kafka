package fr.orderflow.common.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Reglages des reprises sur erreur, communs aux services ({@code orderflow.kafka.retry.*}).
 *
 * <p>Deux mecanismes, deux jeux de valeurs :
 * <ul>
 *   <li>{@code blocking} : reprises <b>bloquantes</b> d'un {@code DefaultErrorHandler}. Le consumer
 *       re-essaie le message sur place, la partition est occupee pendant ce temps. Delais courts,
 *       adaptes aux erreurs qui se resorbent en quelques millisecondes (verrou optimiste).</li>
 *   <li>{@code topics} : reprises <b>non bloquantes</b> par topics de retry
 *       ({@code <topic>-retry-0}, {@code -retry-1}) puis {@code <topic>.DLT}. La partition continue
 *       d'avancer pendant l'attente. Delais longs, adaptes a un tiers indisponible.</li>
 * </ul>
 *
 * <p>Les tests les raccourcissent (cf. {@code src/test/resources/application.yml}).
 */
@ConfigurationProperties(prefix = "orderflow.kafka.retry")
public record KafkaRetryProperties(
        @DefaultValue Blocking blocking,
        @DefaultValue Topics topics) {

    /**
     * Reprises bloquantes : {@code maxRetries} reprises apres la premiere tentative.
     */
    public record Blocking(
            @DefaultValue("200ms") Duration initialInterval,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("2s") Duration maxInterval,
            @DefaultValue("4") int maxRetries) {
    }

    /**
     * Reprises par topics : {@code maxAttempts} tentatives au total, premiere comprise
     * (3 = le message d'origine puis {@code -retry-0} et {@code -retry-1}).
     */
    public record Topics(
            @DefaultValue("1s") Duration initialInterval,
            @DefaultValue("2.0") double multiplier,
            @DefaultValue("8s") Duration maxInterval,
            @DefaultValue("3") int maxAttempts) {
    }
}
