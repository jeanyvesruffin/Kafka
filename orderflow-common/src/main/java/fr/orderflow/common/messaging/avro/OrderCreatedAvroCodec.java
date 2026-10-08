package fr.orderflow.common.messaging.avro;

import fr.orderflow.common.avro.OrderCreated;
import fr.orderflow.common.avro.OrderCreatedLine;
import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import org.apache.avro.Schema;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.BinaryMessageEncoder;
import org.apache.avro.message.SchemaStore;
import org.apache.avro.util.ClassSecurityValidator;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;

/**
 * Encodage et decodage Avro de {@link OrderCreatedEvent} (phase 7, topic {@code orders.created}).
 *
 * <h3>Format sur le fil : "single-object encoding"</h3>
 * Chaque message est {@code C3 01} + l'empreinte (CRC-64-AVRO, 8 octets) du schema <b>d'ecriture</b> +
 * les donnees en binaire Avro. L'empreinte joue le role de l'identifiant de schema d'un registre : le
 * lecteur sait avec quelle version le message a ete ecrit sans qu'elle soit recopiee dans chaque
 * message.
 *
 * <h3>Evolution de schema sans registre</h3>
 * Un registre de schemas (Apicurio, Confluent) resout l'empreinte vers le schema. Ici, c'est un
 * {@link SchemaStore} local qui contient <b>toutes les versions connues</b> : la version courante
 * (classe generee) et celles de {@code classpath:avro/history/OrderCreated.*.avsc}. Le decodeur lit
 * avec le schema courant et applique la <i>resolution Avro</i> : un champ ajoute avec valeur par defaut
 * prend sa valeur par defaut quand le message vient d'une version plus ancienne.
 *
 * <p>Limite assumee de l'approche sans registre : un consommateur ne sait lire que les versions livrees
 * avec son propre code. Un message ecrit avec un schema inconnu est rejete (voir
 * {@link #decode(byte[])}). Ajouter une version = ajouter son fichier {@code .avsc} dans
 * {@code avro/history} <i>et</i> deployer les consommateurs avant les producteurs.
 *
 * <p>Thread-safe : l'encodeur et le decodeur d'Avro le sont.
 */
public class OrderCreatedAvroCodec {

    private static final String HISTORY_PATTERN = "classpath*:avro/history/OrderCreated.*.avsc";
    private static final int MONEY_SCALE = 2;

    static {
        // Depuis Avro 1.11.4 / 1.12, resoudre le schema d'un message vers sa classe Java est refuse pour
        // toute classe qui n'est pas "de confiance" (protection contre la deserialisation de classes
        // arbitraires designees par un schema hostile). Sans cette ligne : SecurityException "Forbidden
        // fr.orderflow.common.avro.OrderCreated". On ne fait confiance qu'au paquet des classes
        // generees par ce module, en conservant les regles deja en place.
        ClassSecurityValidator.setGlobal(ClassSecurityValidator.composite(
                ClassSecurityValidator.getGlobal(),
                type -> OrderCreated.class.getPackageName().equals(type.getPackageName())));
    }

    private final BinaryMessageEncoder<OrderCreated> encoder = OrderCreated.getEncoder();
    private final BinaryMessageDecoder<OrderCreated> decoder;

    public OrderCreatedAvroCodec() {
        this.decoder = OrderCreated.createDecoder(knownSchemas());
    }

    /**
     * Toutes les versions du schema, version courante comprise.
     */
    public static SchemaStore.Cache knownSchemas() {
        SchemaStore.Cache store = new SchemaStore.Cache();
        store.addSchema(OrderCreated.getClassSchema());
        try {
            for (Resource resource : new PathMatchingResourcePatternResolver().getResources(HISTORY_PATTERN)) {
                try (InputStream in = resource.getInputStream()) {
                    store.addSchema(new Schema.Parser().parse(in));
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Schemas Avro historiques illisibles", e);
        }
        return store;
    }

    public byte[] encode(OrderCreatedEvent event) {
        OrderCreated avro = OrderCreated.newBuilder()
                .setEventId(event.eventId())
                .setOrderId(event.orderId())
                .setCustomerId(event.customerId())
                .setItems(event.items().stream().map(OrderCreatedAvroCodec::toAvro).toList())
                .setTotalAmount(money(event.totalAmount()))
                .setOccurredAt(event.occurredAt().truncatedTo(ChronoUnit.MILLIS))
                .setCouponCode(event.couponCode())
                .build();
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            encoder.encode(avro, out);
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException("Encodage Avro impossible orderId=" + event.orderId(), e);
        }
    }

    /**
     * @throws org.apache.avro.message.BadHeaderException     le message ne commence pas par {@code C3 01}
     *                                                        (autre format, JSON par exemple)
     * @throws org.apache.avro.message.MissingSchemaException l'empreinte ne correspond a aucune version connue
     * @throws IOException                                    donnees tronquees ou corrompues
     */
    public OrderCreatedEvent decode(byte[] bytes) throws IOException {
        OrderCreated avro = decoder.decode(bytes);
        return new OrderCreatedEvent(
                avro.getEventId(),
                avro.getOrderId(),
                avro.getCustomerId(),
                avro.getItems().stream()
                        .map(line -> new OrderLine(line.getProductId(), line.getQuantity(), line.getUnitPrice()))
                        .toList(),
                avro.getTotalAmount(),
                avro.getOccurredAt(),
                avro.getCouponCode());
    }

    private static OrderCreatedLine toAvro(OrderLine line) {
        return OrderCreatedLine.newBuilder()
                .setProductId(line.productId())
                .setQuantity(line.quantity())
                .setUnitPrice(money(line.unitPrice()))
                .build();
    }

    /**
     * Le type {@code decimal(12,2)} d'Avro refuse une echelle differente : 39,8 devient 39,80. Un montant
     * qui aurait plus de deux decimales leve une {@link ArithmeticException} plutot que d'etre arrondi
     * en silence.
     */
    private static BigDecimal money(BigDecimal amount) {
        return amount.setScale(MONEY_SCALE, RoundingMode.UNNECESSARY);
    }
}
