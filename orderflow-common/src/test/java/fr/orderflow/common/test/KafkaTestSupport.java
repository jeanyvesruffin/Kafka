package fr.orderflow.common.test;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/**
 * Outils des tests d'integration {@code @EmbeddedKafka}, partages entre les services via le
 * {@code test-jar} d'orderflow-common.
 *
 * <p>Le test joue le role de l'autre service : il publie sur un topic avec son propre producteur
 * (donc <i>sans passer par le code du service teste</i>, y compris pour le format sur le fil), et lit
 * ce qui arrive sur un autre topic avec son propre consumer. Les valeurs sont des {@code byte[]}
 * pour servir aussi bien le JSON que l'Avro et les messages corrompus.
 *
 * <p>Le consumer de lecture utilise {@code assign} plutot que {@code subscribe} : pas de consumer
 * group, donc pas d'attente de rebalance, et une lecture qui s'arrete au dernier offset.
 */
public final class KafkaTestSupport {

    public static final Duration TIMEOUT = Duration.ofSeconds(30);

    private final String brokers;

    /**
     * @param brokers valeur de {@code spring.embedded.kafka.brokers} : avec le broker KRaft embarque
     *                le port n'est pas fixe, il ne faut jamais ecrire {@code localhost:9092} en dur
     */
    public KafkaTestSupport(String brokers) {
        this.brokers = brokers;
    }

    /**
     * Valeur d'un en-tete en texte UTF-8, ou null s'il est absent.
     */
    public static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null || header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }

    /**
     * Les en-tetes du message, {@code nom=valeur} (valeurs binaires tronquees) : pour un message
     * d'assertion lisible quand un en-tete attendu est absent.
     */
    public static String describeHeaders(ConsumerRecord<?, ?> record) {
        StringBuilder out = new StringBuilder();
        for (Header header : record.headers()) {
            String value = header.value() == null ? "null" : new String(header.value(), StandardCharsets.UTF_8);
            out.append(header.key()).append('=').append(value.length() > 80 ? value.substring(0, 80) + "..." : value)
                    .append("; ");
        }
        return out.toString();
    }

    /**
     * Valeur d'un message texte (JSON).
     */
    public static String text(ConsumerRecord<String, byte[]> record) {
        return new String(record.value(), StandardCharsets.UTF_8);
    }

    public void send(String topic, String key, String payload, Map<String, String> headers) {
        send(topic, key, payload.getBytes(StandardCharsets.UTF_8), headers);
    }

    public void send(String topic, String key, byte[] payload, Map<String, String> headers) {
        try (Producer<String, byte[]> producer = new KafkaProducer<>(
                KafkaTestUtils.producerProps(brokers), new StringSerializer(), new ByteArraySerializer())) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, key, payload);
            headers.forEach((name, value) -> record.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));
            producer.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException("Envoi impossible topic=" + topic, e);
        }
    }

    /**
     * Premier message portant cette cle, en attendant qu'il arrive.
     */
    public ConsumerRecord<String, byte[]> awaitRecord(String topic, String key) {
        return await().atMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(300))
                .until(() -> records(topic, key), found -> !found.isEmpty())
                .getFirst();
    }

    /**
     * Tous les messages du topic portant cette cle, lus du debut jusqu'a la fin (liste vide si le
     * topic n'existe pas encore ou ne contient rien pour cette cle).
     */
    public List<ConsumerRecord<String, byte[]>> records(String topic, String key) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(brokers, "embedded-test", false);
        try (Consumer<String, byte[]> consumer = new KafkaConsumer<>(
                props, new StringDeserializer(), new ByteArrayDeserializer())) {
            var partitionInfos = consumer.listTopics().get(topic);
            if (partitionInfos == null) {
                return List.of();
            }
            List<TopicPartition> partitions = partitionInfos.stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            List<ConsumerRecord<String, byte[]>> records = new ArrayList<>();
            while (partitions.stream().anyMatch(tp -> consumer.position(tp) < endOffsets.get(tp))) {
                consumer.poll(Duration.ofMillis(100))
                        .records(topic)
                        .forEach(record -> {
                            if (key.equals(record.key())) {
                                records.add(record);
                            }
                        });
            }
            return records;
        }
    }
}
