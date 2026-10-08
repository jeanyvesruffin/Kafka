import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

/**
 * Rejoue un message, comme le bouton "Republish" d'AKHQ : relit le PREMIER message portant la cle donnee
 * et le republie a l'identique (cle, octets de la valeur, en-tetes).
 *
 * <pre>
 *   java -cp "&lt;kafka-clients.jar&gt;;&lt;slf4j-api.jar&gt;" scripts/ReplayMessage.java &lt;bootstrap&gt; &lt;topic&gt; &lt;cle&gt; [topic-cible]
 * </pre>
 *
 * <ul>
 *   <li>sans {@code topic-cible} : republie sur le meme topic (rejouer un doublon pour verifier
 *       l'idempotence, phase 3) ;</li>
 *   <li>avec {@code topic-cible} : republie ailleurs, typiquement du DLT vers le topic d'origine une fois
 *       la cause corrigee (phase 4). Seuls les en-tetes metier ({@code eventId}, {@code eventType},
 *       {@code correlationId}, {@code contentType}) sont conserves : ceux du DLT decrivent l'echec, pas le
 *       message.</li>
 * </ul>
 *
 * Les octets de la valeur sont copies tels quels : cela marche aussi pour {@code orders.created}, en Avro.
 * Les jars se trouvent dans le dossier {@code libs} d'une distribution Kafka, ou dans le depot Maven local
 * ({@code ~/.m2/repository/org/apache/kafka/kafka-clients/4.2.1}).
 */
public class ReplayMessage {

    private static final Set<String> BUSINESS_HEADERS = Set.of("eventId", "eventType", "correlationId", "contentType");

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage : ReplayMessage <bootstrap> <topic> <cle> [topic-cible]");
            System.exit(64);
        }
        String bootstrap = args[0];
        String topic = args[1];
        String key = args[2];
        String target = args.length > 3 ? args[3] : topic;

        ConsumerRecord<String, byte[]> found = find(bootstrap, topic, key);
        if (found == null) {
            System.err.println("Message introuvable : " + topic + " / " + key);
            System.exit(2);
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        try (KafkaProducer<String, byte[]> producer =
                     new KafkaProducer<>(props, new StringSerializer(), new ByteArraySerializer())) {
            ProducerRecord<String, byte[]> copy = new ProducerRecord<>(target, null, found.key(), found.value());
            found.headers().forEach(header -> {
                if (target.equals(topic) || BUSINESS_HEADERS.contains(header.key())) {
                    copy.headers().add(header.key(), header.value());
                }
            });
            RecordMetadata sent = producer.send(copy).get();
            System.out.printf("Rejoue %s => %s key=%s (origine partition %d offset %d) -> partition %d offset %d%n",
                    topic, target, key, found.partition(), found.offset(), sent.partition(), sent.offset());
        }
    }

    private static ConsumerRecord<String, byte[]> find(String bootstrap, String topic, String key) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "replay-" + UUID.randomUUID());
        props.put("enable.auto.commit", "false");
        try (KafkaConsumer<String, byte[]> consumer =
                     new KafkaConsumer<>(props, new StringDeserializer(), new ByteArrayDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
            while (partitions.stream().anyMatch(tp -> consumer.position(tp) < end.get(tp))) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200)).records(topic)) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
            return null;
        }
    }
}
