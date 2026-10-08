package fr.orderflow.common.kafka;

import fr.orderflow.common.event.OrderCreatedEvent;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.RecordInterceptor;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;

import java.util.Map;

/**
 * Support Kafka commun aux services, actif uniquement avec {@code orderflow.messaging.publisher: kafka}
 * et si Spring Kafka est present.
 *
 * <p>Le reste du module commun ne depend pas de Kafka (contrats d'evenements, port
 * {@code EventPublisher}) : tout ce qui est specifique au transport vit dans ce package, que les
 * services ne voient qu'a travers leurs propres beans ({@code DefaultErrorHandler}, topics).
 */
@Configuration
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
@EnableConfigurationProperties(KafkaRetryProperties.class)
public class KafkaSupportConfig {

    /**
     * Le {@code KafkaTemplate} de Boot sait publier des {@code String} <b>et</b> des {@code byte[]}.
     *
     * <p>Le Dead Letter Publishing Recoverer republie un message dont la deserialisation a echoue
     * avec ses <b>octets d'origine</b> ({@code byte[]}). Avec le seul {@code StringSerializer}, cette
     * publication echouerait sur un {@code ClassCastException}, et le message empoisonne bloquerait
     * le consumer au lieu de partir en DLT. {@code DelegatingByTypeSerializer} choisit le serialiseur
     * selon le type de la valeur ; les publishers JSON continuent d'envoyer des {@code String}
     * sans rien changer. Les messages {@code orders.created} qui ont ete deserialises avant d'echouer
     * dans le traitement sont, eux, republies en Avro.
     */
    @Bean
    DefaultKafkaProducerFactoryCustomizer orderflowStringAndBytesSerializers() {
        return producerFactory -> {
            @SuppressWarnings("unchecked")
            DefaultKafkaProducerFactory<Object, Object> factory =
                    (DefaultKafkaProducerFactory<Object, Object>) producerFactory;
            factory.setKeySerializerSupplier(KafkaSupportConfig::stringAndBytes);
            factory.setValueSerializerSupplier(KafkaSupportConfig::stringAndBytes);
        };
    }

    /**
     * correlationId de l'en-tete Kafka -> MDC des logs (phase 5). Boot l'applique tout seul a la
     * fabrique de conteneurs, a condition qu'il n'y ait qu'un seul {@code RecordInterceptor}.
     */
    @Bean
    RecordInterceptor<Object, Object> orderflowCorrelationIdInterceptor() {
        return new CorrelationIdRecordInterceptor<>();
    }

    private static Serializer<Object> stringAndBytes() {
        return new DelegatingByTypeSerializer(Map.of(
                byte[].class, new ByteArraySerializer(),
                String.class, new StringSerializer(),
                // Valeur d'un ConsumerRecord de orders.created en echec de traitement : l'objet deja
                // deserialise, a reserialiser pour le DLT (voir OrderCreatedAvroSerializer)
                OrderCreatedEvent.class, new OrderCreatedAvroSerializer()));
    }
}
