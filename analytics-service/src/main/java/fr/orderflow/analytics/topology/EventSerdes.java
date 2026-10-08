package fr.orderflow.analytics.topology;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderFlowEvent;
import fr.orderflow.common.kafka.OrderCreatedAvroDeserializer;
import fr.orderflow.common.kafka.OrderCreatedAvroSerializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

/**
 * Serdes de la topologie : Kafka Streams lit et ecrit des octets, il faut lui dire comment passer
 * des octets aux objets (et inversement) pour chaque topic et chaque state store.
 *
 * <p>Les formats sont ceux du fil : Avro pour {@code orders.created} (phase 7), JSON pour le reste.
 * Une erreur de lecture est transmise a Kafka Streams, qui applique son
 * {@code deserialization.exception.handler} (voir {@code application.yml}) : le message part dans
 * {@code orderflow-analytics.DLT} au lieu d'arreter l'application.
 */
public final class EventSerdes {

    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private EventSerdes() {
    }

    public static Serde<OrderCreatedEvent> orderCreated() {
        return Serdes.serdeFrom(new OrderCreatedAvroSerializer(), new OrderCreatedAvroDeserializer());
    }

    public static <T extends OrderFlowEvent> Serde<T> json(Class<T> type) {
        return Serdes.serdeFrom(
                (topic, value) -> value == null ? null : MAPPER.writeValueAsBytes(value),
                (topic, bytes) -> bytes == null ? null : MAPPER.readValue(bytes, type));
    }

    /**
     * Montants des state stores : texte decimal ("39.80"), lisible dans les topics changelog.
     */
    public static Serde<BigDecimal> bigDecimal() {
        return Serdes.serdeFrom(
                (topic, value) -> value == null ? null : value.toPlainString().getBytes(StandardCharsets.UTF_8),
                (topic, bytes) -> bytes == null ? null : new BigDecimal(new String(bytes, StandardCharsets.UTF_8)));
    }
}
