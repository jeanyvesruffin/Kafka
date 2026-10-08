package fr.orderflow.common.kafka;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.messaging.avro.OrderCreatedAvroCodec;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serialiseur Kafka d'{@link OrderCreatedEvent} en Avro, symetrique de {@link OrderCreatedAvroDeserializer}.
 *
 * <p>Il n'est pas utilise par le publisher d'order-service (qui envoie directement des octets), mais
 * par le <b>Dead Letter Publishing Recoverer</b>. Quand un message a ete deserialise avec succes puis a
 * echoue dans le traitement (verrou optimiste jamais libere par exemple), la valeur du
 * {@code ConsumerRecord} en echec est l'objet {@code OrderCreatedEvent}, pas les octets d'origine :
 * pour le republier sur {@code orders.created.DLT}, il faut savoir le reserialiser. Sans cela, la
 * publication en DLT echoue ("No matching delegate") et le message reste coince dans le consumer.
 *
 * <p>Le DLT recoit donc des octets Avro equivalents a ceux d'origine (au plus, l'horodatage est
 * tronque a la milliseconde, comme dans le message initial).
 */
public class OrderCreatedAvroSerializer implements Serializer<OrderCreatedEvent> {

    private final OrderCreatedAvroCodec codec = new OrderCreatedAvroCodec();

    @Override
    public byte[] serialize(String topic, OrderCreatedEvent event) {
        if (event == null) {
            return null;
        }
        try {
            return codec.encode(event);
        } catch (RuntimeException e) {
            throw new SerializationException("Encodage Avro impossible sur " + topic + " : " + e.getMessage(), e);
        }
    }
}
