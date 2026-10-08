package fr.orderflow.common.kafka;

import fr.orderflow.common.messaging.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.time.Duration;
import java.util.Arrays;

/**
 * Declaration des Dead Letter Topics d'un service.
 *
 * <p>Chaque service declare le {@code <topic>.DLT} de chacun des topics qu'il <b>consomme</b> : c'est
 * lui qui y publie, comme chaque service declare les topics qu'il produit. Sans declaration, le
 * broker creerait le DLT a la volee avec ses reglages par defaut, ou refuserait de le creer sur un
 * cluster ou la creation automatique est coupee.
 *
 * <p>Meme nombre de partitions que les topics sources : le {@code DeadLetterPublishingRecoverer}
 * republie par defaut sur le <b>meme numero de partition</b> que le message d'origine. Un DLT a une
 * seule partition ferait echouer toute publication issue des partitions 1 et 2.
 *
 * <p>Retention de 14 jours (7 pour les topics metier) : le temps d'investiguer et de rejouer.
 */
public final class DeadLetterTopics {

    public static final int PARTITIONS = 3;
    public static final short REPLICAS = 1; // un seul broker en local
    public static final Duration RETENTION = Duration.ofDays(14);

    private DeadLetterTopics() {
    }

    public static KafkaAdmin.NewTopics of(String... sourceTopics) {
        NewTopic[] deadLetterTopics = Arrays.stream(sourceTopics)
                .map(source -> TopicBuilder.name(Topics.dltOf(source))
                        .partitions(PARTITIONS)
                        .replicas(REPLICAS)
                        .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(RETENTION.toMillis()))
                        .build())
                .toArray(NewTopic[]::new);
        return new KafkaAdmin.NewTopics(deadLetterTopics);
    }
}
