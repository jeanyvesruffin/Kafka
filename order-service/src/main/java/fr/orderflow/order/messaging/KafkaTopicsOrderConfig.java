package fr.orderflow.order.messaging;


import fr.orderflow.common.messaging.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicsOrderConfig {

    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 1; // un seul broker en local

    @Bean
    public KafkaAdmin.NewTopics orderTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(Topics.ORDERS_CREATED)
                        .partitions(PARTITIONS)
                        .replicas(REPLICAS)
                        .build(),
                TopicBuilder.name(Topics.ORDERS_CONFIRMED)
                        .partitions(PARTITIONS)
                        .replicas(REPLICAS)
                        .build(),
                TopicBuilder.name(Topics.ORDERS_CANCELLED)
                        .partitions(PARTITIONS)
                        .replicas(REPLICAS)
                        .build());
    }
}
