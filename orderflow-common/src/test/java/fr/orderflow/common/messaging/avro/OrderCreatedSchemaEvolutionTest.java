package fr.orderflow.common.messaging.avro;

import fr.orderflow.common.avro.OrderCreated;
import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.test.OrderCreatedV1;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.SchemaStore;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Evolution du schema d'{@code OrderCreated} (phase 7) : ajouter un champ optionnel sans casser personne.
 *
 * <p>Vocabulaire. Compatibilite <b>BACKWARD</b> : le <i>nouveau</i> schema sait lire des donnees ecrites
 * avec l'<i>ancien</i> (on met a jour les consommateurs en premier). Compatibilite <b>FORWARD</b> :
 * l'<i>ancien</i> schema sait lire des donnees ecrites avec le <i>nouveau</i> (un consommateur pas encore
 * redeploye lit les messages d'un producteur deja migre). Un champ ajoute avec une valeur par defaut
 * donne les deux ; c'est cette propriete (<b>FULL</b>) que ces tests verrouillent.
 *
 * <p>Le premier test est une barriere pour l'avenir : toute nouvelle version du schema doit rester
 * compatible avec <b>toutes</b> les versions deja livrees ({@code avro/history}), sinon le build casse.
 */
class OrderCreatedSchemaEvolutionTest {

    private static final Schema CURRENT = OrderCreated.getClassSchema();
    private static final Schema V1 = OrderCreatedV1.schema();

    private static List<Schema> history() throws IOException {
        List<Schema> versions = new ArrayList<>();
        for (Resource resource : new PathMatchingResourcePatternResolver()
                .getResources("classpath*:avro/history/OrderCreated.*.avsc")) {
            try (InputStream in = resource.getInputStream()) {
                versions.add(new Schema.Parser().parse(in));
            }
        }
        return versions;
    }

    private static SchemaCompatibilityType compatibility(Schema reader, Schema writer) {
        return SchemaCompatibility.checkReaderWriterCompatibility(reader, writer).getType();
    }

    private static Schema schemaWith(String extraFieldJson) {
        String json = """
                {"type":"record","name":"OrderCreated","namespace":"fr.orderflow.common.avro","fields":[
                  {"name":"eventId","type":"string"},
                  {"name":"orderId","type":"string"}%s
                ]}""".formatted(extraFieldJson);
        return new Schema.Parser().parse(json);
    }

    @Test
    @DisplayName("Barriere : le schema courant reste compatible (BACKWARD et FORWARD) avec TOUTES les versions deja livrees")
    void currentSchema_isCompatibleWithEveryShippedVersion() throws IOException {
        List<Schema> shipped = history();

        assertThat(shipped).as("au moins la version 1 est conservee dans avro/history").isNotEmpty();
        for (Schema old : shipped) {
            assertThat(compatibility(CURRENT, old))
                    .as("BACKWARD : le schema courant lit les messages de %s", old.getFullName())
                    .isEqualTo(SchemaCompatibilityType.COMPATIBLE);
            assertThat(compatibility(old, CURRENT))
                    .as("FORWARD : l'ancien schema lit les messages du schema courant")
                    .isEqualTo(SchemaCompatibilityType.COMPATIBLE);
        }
    }

    @Test
    @DisplayName("Ajouter un champ OPTIONNEL (union [null, string], defaut null) : compatible dans les deux sens")
    void addingAnOptionalField_isFullyCompatible() {
        Schema before = schemaWith("");
        Schema after = schemaWith(",{\"name\":\"couponCode\",\"type\":[\"null\",\"string\"],\"default\":null}");

        assertThat(compatibility(after, before)).as("BACKWARD").isEqualTo(SchemaCompatibilityType.COMPATIBLE);
        assertThat(compatibility(before, after)).as("FORWARD").isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    @DisplayName("Ajouter un champ OBLIGATOIRE sans defaut : casse BACKWARD (le nouveau schema ne sait pas lire l'ancien)")
    void addingARequiredField_breaksBackwardCompatibility() {
        Schema before = schemaWith("");
        Schema after = schemaWith(",{\"name\":\"couponCode\",\"type\":\"string\"}");

        assertThat(compatibility(after, before)).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
        // En revanche l'ancien schema ignore simplement le champ en plus : FORWARD tient
        assertThat(compatibility(before, after)).isEqualTo(SchemaCompatibilityType.COMPATIBLE);
    }

    @Test
    @DisplayName("Changer le type d'un champ (int -> string) : incompatible dans les deux sens")
    void changingAFieldType_breaksCompatibility() {
        Schema asInt = schemaWith(",{\"name\":\"quantity\",\"type\":\"int\"}");
        Schema asString = schemaWith(",{\"name\":\"quantity\",\"type\":\"string\"}");

        assertThat(compatibility(asString, asInt)).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
        assertThat(compatibility(asInt, asString)).isEqualTo(SchemaCompatibilityType.INCOMPATIBLE);
    }

    @Test
    @DisplayName("BACKWARD en pratique : un message de la version 1 est lu avec le schema courant, couponCode prend son defaut")
    void backward_currentReaderReadsVersion1Message() throws IOException {
        OrderCreatedEvent event = new OrderCreatedEvent(
                "evt-1", "ord-1", "cust-118", List.of(new OrderLine("sku-001", 2, new BigDecimal("19.90"))),
                new BigDecimal("39.80"), Instant.parse("2026-10-08T10:15:30Z"));

        OrderCreated read = OrderCreated.createDecoder(OrderCreatedAvroCodec.knownSchemas())
                .decode(OrderCreatedV1.encode(event));

        assertThat(read.getOrderId()).isEqualTo("ord-1");
        assertThat(read.getCouponCode()).as("champ absent du message : valeur par defaut du schema").isNull();
    }

    @Test
    @DisplayName("FORWARD en pratique : un consommateur reste sur le schema v1 et lit un message v2 (le coupon est ignore)")
    void forward_versionOneReaderReadsVersion2Message() throws IOException {
        OrderCreatedEvent event = new OrderCreatedEvent(
                "evt-2", "ord-2", "cust-118", List.of(new OrderLine("sku-001", 2, new BigDecimal("19.90"))),
                new BigDecimal("39.80"), Instant.parse("2026-10-08T10:15:30Z"), "PROMO10");
        byte[] version2Message = new OrderCreatedAvroCodec().encode(event);

        // Lecteur "pas redeploye" : schema de lecture = v1. Il lui faut malgre tout connaitre le schema
        // d'ECRITURE (v2) pour decoder : un registre le lui fournirait ; ici c'est le SchemaStore.
        GenericData model = new GenericData();
        model.addLogicalTypeConversion(new Conversions.DecimalConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());
        SchemaStore.Cache store = OrderCreatedAvroCodec.knownSchemas();
        GenericRecord read = new BinaryMessageDecoder<GenericRecord>(model, V1, store).decode(version2Message);

        assertThat(read.get("orderId")).hasToString("ord-2");   // Utf8 en API generique
        assertThat(read.getSchema().getField("couponCode")).as("le lecteur v1 ne connait pas ce champ").isNull();
    }
}
