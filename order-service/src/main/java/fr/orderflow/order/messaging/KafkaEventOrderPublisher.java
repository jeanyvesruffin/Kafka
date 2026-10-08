package fr.orderflow.order.messaging;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventPublisher;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.common.messaging.avro.OrderCreatedAvroCodec;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publisher Kafka du service Commande.
 *
 * <p>Le format du fil est une affaire de transport, donc de cet adaptateur : l'outbox garde l'evenement
 * en JSON (lisible, independant du format Kafka) et c'est ici que {@code orders.created} est converti en
 * <b>Avro</b> (phase 7, voir {@link OrderCreatedAvroCodec}). Les autres topics restent en JSON. Convertir
 * a la publication permet aussi de changer de format sans migrer les lignes deja presentes dans l'outbox.
 */
@Component
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
@RequiredArgsConstructor
public class KafkaEventOrderPublisher implements EventPublisher {

    static final String AVRO_CONTENT_TYPE = "application/avro";

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventSerializer eventSerializer;
    private final OrderCreatedAvroCodec avroCodec;


    @Override
    public void publish(EventEnvelope envelope) {

        Object value = envelope.payload();
        Map<String, String> headers = envelope.headers();
        if (Topics.isAvro(envelope.topic())) {
            value = avroCodec.encode(eventSerializer.fromJson(envelope.payload(), OrderCreatedEvent.class));
            headers = new LinkedHashMap<>(headers);
            headers.put(EventHeaders.CONTENT_TYPE, AVRO_CONTENT_TYPE);
        }

        ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(
                envelope.topic(),
                envelope.key(),
                value);

        headers.forEach((headerName, headerValue) -> producerRecord.headers()
                .add(headerName, headerValue.getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(producerRecord)
                    .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread()
                    .interrupt();
            throw new KafkaException("Envoi interrompu topic=" + envelope.topic(), e);
        } catch (ExecutionException e) {
            throw new KafkaException(
                    "Envoi refuse par le broker topic=" + envelope.topic() + " : " + e.getCause()
                            .getMessage(), e.getCause());
        } catch (TimeoutException e) {
            throw new KafkaException(
                    "Pas d'acquittement du broker sous " + SEND_TIMEOUT_SECONDS + " s topic=" + envelope.topic(), e);
        }
    }
}
