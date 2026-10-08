import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * Publie un message TEXTE avec des en-tetes : utile pour injecter un message empoisonne (JSON illisible)
 * et observer le Dead Letter Topic (phase 4).
 *
 * <pre>
 *   java -cp "&lt;kafka-clients.jar&gt;;&lt;slf4j-api.jar&gt;" scripts/ProduceMessage.java &lt;bootstrap&gt; &lt;topic&gt; &lt;cle&gt; &lt;valeur&gt; [nom=valeur ...]
 * </pre>
 *
 * Exemple (poison pill sur inventory.reserved, lu par order-service et payment-service) :
 * <pre>
 *   ... ProduceMessage.java localhost:9092 inventory.reserved ord-poison '{"eventId": pas du json' correlationId=corr-poison
 * </pre>
 */
public class ProduceMessage {

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage : ProduceMessage <bootstrap> <topic> <cle> <valeur> [nom=valeur ...]");
            System.exit(64);
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", args[0]);
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(props, new StringSerializer(), new StringSerializer())) {
            ProducerRecord<String, String> record = new ProducerRecord<>(args[1], args[2], args[3]);
            for (int i = 4; i < args.length; i++) {
                String[] header = args[i].split("=", 2);
                record.headers().add(header[0], header[1].getBytes(StandardCharsets.UTF_8));
            }
            RecordMetadata sent = producer.send(record).get();
            System.out.printf("Publie %s key=%s -> partition %d offset %d%n", args[1], args[2], sent.partition(), sent.offset());
        }
    }
}
