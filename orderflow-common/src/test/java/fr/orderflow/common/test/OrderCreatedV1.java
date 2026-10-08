package fr.orderflow.common.test;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.BinaryMessageEncoder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.RoundingMode;
import java.time.temporal.ChronoUnit;
import java.util.List;

/**
 * Joue un <b>producteur de la version 1</b> du contrat {@code OrderCreated} (sans {@code couponCode}).
 *
 * <p>Pour verifier qu'un consommateur a jour lit toujours les messages d'un producteur qui n'a pas ete
 * redeploye : il ecrit avec le schema {@code avro/history/OrderCreated.v1.avsc}, a travers l'API
 * generique d'Avro, sans aucune des classes generees de la version courante.
 */
public final class OrderCreatedV1 {

    private static final String RESOURCE = "/avro/history/OrderCreated.v1.avsc";

    private OrderCreatedV1() {
    }

    public static Schema schema() {
        try (InputStream in = OrderCreatedV1.class.getResourceAsStream(RESOURCE)) {
            return new Schema.Parser().parse(in);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Le message tel que l'aurait publie la version 1 (le {@code couponCode} de l'evenement est ignore).
     */
    public static byte[] encode(OrderCreatedEvent event) {
        Schema schema = schema();
        GenericData model = new GenericData();
        model.addLogicalTypeConversion(new Conversions.DecimalConversion());
        model.addLogicalTypeConversion(new TimeConversions.TimestampMillisConversion());

        Schema lineSchema = schema.getField("items").schema().getElementType();
        List<GenericRecord> lines = event.items().stream().map(line -> line(lineSchema, line)).toList();
        GenericRecord record = new GenericData.Record(schema);
        record.put("eventId", event.eventId());
        record.put("orderId", event.orderId());
        record.put("customerId", event.customerId());
        record.put("items", lines);
        record.put("totalAmount", event.totalAmount().setScale(2, RoundingMode.UNNECESSARY));
        record.put("occurredAt", event.occurredAt().truncatedTo(ChronoUnit.MILLIS));
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            new BinaryMessageEncoder<GenericRecord>(model, schema).encode(record, out);
            return out.toByteArray();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static GenericRecord line(Schema lineSchema, OrderLine line) {
        GenericRecord record = new GenericData.Record(lineSchema);
        record.put("productId", line.productId());
        record.put("quantity", line.quantity());
        record.put("unitPrice", line.unitPrice().setScale(2, RoundingMode.UNNECESSARY));
        return record;
    }
}
