package fr.orderflow.payment.messaging;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.StaticListableBeanFactory;
import org.springframework.kafka.support.KafkaHeaders;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * inventory.reserved.DLT est lu par deux services (order-service et payment-service consomment le meme
 * topic) : le handler de payment ne doit compter que ses propres echecs.
 */
class PaymentDeadLetterHandlerTest {

    private MeterRegistry registry;
    private PaymentDeadLetterHandler handler;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        StaticListableBeanFactory beans = new StaticListableBeanFactory();
        beans.addBean("registry", registry);
        handler = new PaymentDeadLetterHandler(beans.getBeanProvider(MeterRegistry.class));
    }

    private static ConsumerRecord<String, String> deadLetter(String consumerGroup) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>("inventory.reserved.DLT", 0, 0, "ord-1", "{}");
        record.headers().add(KafkaHeaders.ORIGINAL_TOPIC, "inventory.reserved".getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.ORIGINAL_CONSUMER_GROUP, consumerGroup.getBytes(StandardCharsets.UTF_8));
        record.headers().add(KafkaHeaders.EXCEPTION_CAUSE_FQCN,
                "fr.orderflow.payment.service.PaymentGatewayUnavailableException".getBytes(StandardCharsets.UTF_8));
        return record;
    }

    @Test
    @DisplayName("Echec de payment-service : compte, avec le topic d'origine et le nom court de l'exception")
    void ownFailure_isCounted() {
        handler.onDeadLetter(deadLetter("payment-service"));

        assertThat(registry.find("orderflow.kafka.dlt")
                .tag("topic", "inventory.reserved")
                .tag("exception", "PaymentGatewayUnavailableException")
                .counter().count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("Echec d'un autre service sur le meme DLT : ignore, rien n'est compte")
    void otherServiceFailure_isIgnored() {
        handler.onDeadLetter(deadLetter("order-service"));

        assertThat(registry.find("orderflow.kafka.dlt").counter()).isNull();
    }
}
