package fr.orderflow.inventory.messaging;

import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventPublisher;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;


@Component
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
public class KafkaEventInventoryPublisher implements EventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaEventInventoryPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @Override
    public void publish(EventEnvelope envelope) {

        ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>(
                envelope.topic(),
                envelope.key(),
                envelope.payload());

        envelope.headers()
                .forEach((stringKey, stringValue) -> stringStringProducerRecord.headers()
                        .add(stringKey, stringValue.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(stringStringProducerRecord);

    }
}
