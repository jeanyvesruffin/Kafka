package fr.orderflow.payment.messaging;


import fr.orderflow.common.messaging.Topics;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration
public class KafkaTopicsPaymentConfig {

    private static final int PARTITIONS = 3;
    private static final int REPLICAS = 1; // un seul broker en local

    @Bean
    public KafkaAdmin.NewTopics paymentTopics() {
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name(Topics.PAYMENTS_COMPLETED)
                        .partitions(PARTITIONS)
                        .replicas(REPLICAS)
                        .build(),
                TopicBuilder.name(Topics.PAYMENTS_FAILED)
                        .partitions(PARTITIONS)
                        .replicas(REPLICAS)
                        .build());
    }
}
