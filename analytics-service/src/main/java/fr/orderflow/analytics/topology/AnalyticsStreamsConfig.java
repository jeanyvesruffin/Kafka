package fr.orderflow.analytics.topology;

import fr.orderflow.common.kafka.DeadLetterTopics;
import fr.orderflow.common.messaging.Topics;
import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.context.annotation.Bean;

/**
 * Branchement de la topologie sur Spring Kafka.
 *
 * <p>{@code @EnableKafkaStreams} cree le {@code StreamsBuilder} (partage avec le reste de l'application)
 * et le cycle de vie du {@code KafkaStreams} ; Boot lui fournit sa configuration depuis
 * {@code spring.kafka.streams.*} (voir {@code application.yml}).
 */
@Configuration
@EnableKafkaStreams
public class AnalyticsStreamsConfig {

    private static final int PARTITIONS = DeadLetterTopics.PARTITIONS;
    private static final int REPLICAS = DeadLetterTopics.REPLICAS;

    @Autowired
    void buildTopology(StreamsBuilder builder) {
        AnalyticsTopology.build(builder);
    }

    /**
     * Kafka Streams refuse de demarrer si un topic source n'existe pas ("Source topics do not exist").
     * Le service peut demarrer avant les services qui les creent : il les declare donc lui aussi.
     * Meme reglages que chez leurs proprietaires ; KafkaAdmin ne cree que ce qui manque.
     */
    @Bean
    KafkaAdmin.NewTopics analyticsSourceTopics() {
        return new KafkaAdmin.NewTopics(
                source(Topics.ORDERS_CREATED), source(Topics.ORDERS_CONFIRMED),
                source(Topics.ORDERS_CANCELLED), source(Topics.PAYMENTS_COMPLETED));
    }

    /**
     * Destination des messages illisibles (voir {@code errors.dead.letter.queue.topic.name}).
     */
    @Bean
    KafkaAdmin.NewTopics analyticsDeadLetterTopics() {
        return DeadLetterTopics.of("orderflow-analytics");
    }

    private static org.apache.kafka.clients.admin.NewTopic source(String topic) {
        return TopicBuilder.name(topic).partitions(PARTITIONS).replicas(REPLICAS).build();
    }
}
