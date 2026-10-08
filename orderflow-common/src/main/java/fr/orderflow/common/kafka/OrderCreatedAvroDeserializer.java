package fr.orderflow.common.kafka;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.messaging.avro.OrderCreatedAvroCodec;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * Deserialiseur Kafka des messages Avro de {@code orders.created} (phase 7).
 *
 * <p>Utilise <b>enveloppe par un {@code ErrorHandlingDeserializer}</b> (voir
 * {@code OrderEventListener} d'inventory-service) : un message illisible (JSON de l'ancien format,
 * octets corrompus, schema inconnu) ne leve pas dans la boucle de poll du consumer, ce qui le ferait
 * reboucler sur le meme offset a l'infini. L'echec est transmis au listener container sous forme de
 * {@code DeserializationException}, que le {@code DefaultErrorHandler} classe <b>non retryable</b> et
 * envoie directement sur {@code orders.created.DLT} avec les octets d'origine.
 *
 * <p>Kafka instancie la classe par reflexion : constructeur public sans argument, pas d'injection.
 */
public class OrderCreatedAvroDeserializer implements Deserializer<OrderCreatedEvent> {

    private final OrderCreatedAvroCodec codec = new OrderCreatedAvroCodec();

    @Override
    public OrderCreatedEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return codec.decode(data);
        } catch (IOException | RuntimeException e) {
            throw new SerializationException(
                    "Message Avro illisible sur " + topic + " (" + data.length + " octets) : " + e.getMessage(), e);
        }
    }
}
