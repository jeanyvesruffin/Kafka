package fr.orderflow.payment.messaging;

import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventPublisher;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publisher Kafka du service Paiement.
 *
 * <p>Envoi synchrone : {@code PaymentService} publie depuis sa transaction. Si le
 * broker refuse le message, l'exception annule la transaction (paiement et
 * {@code processed_events} compris) et {@code inventory.reserved} sera relu :
 * aucun resultat de paiement n'est perdu.
 */
@Component
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
@RequiredArgsConstructor
public class KafkaEventPaymentPublisher implements EventPublisher {

    private static final long SEND_TIMEOUT_SECONDS = 10;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Override
    public void publish(EventEnvelope envelope) {

        ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>(
                envelope.topic(),
                envelope.key(),
                envelope.payload());

        envelope.headers()
                .forEach((stringKey, stringValue) -> stringStringProducerRecord.headers()
                        .add(stringKey, stringValue.getBytes(StandardCharsets.UTF_8)));

        try {
            kafkaTemplate.send(stringStringProducerRecord)
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
