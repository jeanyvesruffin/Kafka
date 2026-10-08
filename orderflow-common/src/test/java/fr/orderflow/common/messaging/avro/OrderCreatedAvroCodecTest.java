package fr.orderflow.common.messaging.avro;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.test.OrderCreatedV1;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.message.BadHeaderException;
import org.apache.avro.message.MissingSchemaException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests unitaires purs du codec Avro d'{@code orders.created}.
 */
class OrderCreatedAvroCodecTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:15:30.123456Z");

    private final OrderCreatedAvroCodec codec = new OrderCreatedAvroCodec();

    private static OrderCreatedEvent event(String couponCode) {
        List<OrderLine> items = List.of(
                new OrderLine("sku-001", 2, new BigDecimal("19.90")),
                new OrderLine("sku-003", 4, new BigDecimal("5.00")));
        return new OrderCreatedEvent(
                "evt-1", "ord-1", "cust-118", items, new BigDecimal("59.80"), NOW, couponCode);
    }

    /**
     * L'evenement tel qu'on s'attend a le relire : l'horodatage est tronque a la milliseconde par le
     * type Avro {@code timestamp-millis}.
     */
    private static OrderCreatedEvent afterRoundTrip(OrderCreatedEvent e) {
        return new OrderCreatedEvent(
                e.eventId(), e.orderId(), e.customerId(), e.items(), e.totalAmount(),
                e.occurredAt().truncatedTo(ChronoUnit.MILLIS), e.couponCode());
    }

    @Test
    @DisplayName("Aller-retour avec coupon : tous les champs sont restitues")
    void roundTrip_withCoupon() throws IOException {
        OrderCreatedEvent original = event("PROMO10");

        OrderCreatedEvent decoded = codec.decode(codec.encode(original));

        assertThat(decoded).isEqualTo(afterRoundTrip(original));
        assertThat(decoded.couponCode()).isEqualTo("PROMO10");
    }

    @Test
    @DisplayName("Aller-retour sans coupon : le champ optionnel reste null")
    void roundTrip_withoutCoupon() throws IOException {
        OrderCreatedEvent original = event(null);

        OrderCreatedEvent decoded = codec.decode(codec.encode(original));

        assertThat(decoded).isEqualTo(afterRoundTrip(original));
        assertThat(decoded.couponCode()).isNull();
    }

    @Test
    @DisplayName("Format sur le fil : en-tete 'single-object' C3 01 puis empreinte du schema d'ecriture (8 octets)")
    void wireFormat_startsWithSingleObjectHeader() {
        byte[] bytes = codec.encode(event(null));

        assertThat(bytes[0]).isEqualTo((byte) 0xC3);
        assertThat(bytes[1]).isEqualTo((byte) 0x01);
        assertThat(bytes.length).isGreaterThan(10);
    }

    @Test
    @DisplayName("Message ecrit par la version 1 (sans coupon) : lu par le codec courant, couponCode = null")
    void messageFromVersion1_isReadByTheCurrentCodec() throws IOException {
        OrderCreatedEvent original = event("ignore-en-v1");   // la v1 ne sait pas transporter le coupon

        OrderCreatedEvent decoded = codec.decode(OrderCreatedV1.encode(original));

        assertThat(decoded.couponCode()).isNull();
        assertThat(decoded).isEqualTo(afterRoundTrip(event(null)));
    }

    @Test
    @DisplayName("Les deux versions n'ont pas la meme empreinte : les octets different pour le meme evenement")
    void eachVersionHasItsOwnFingerprint() {
        OrderCreatedEvent e = event(null);

        byte[] v1 = OrderCreatedV1.encode(e);
        byte[] v2 = codec.encode(e);

        assertThat(Arrays.copyOfRange(v1, 2, 10)).isNotEqualTo(Arrays.copyOfRange(v2, 2, 10));
    }

    @Test
    @DisplayName("JSON (ancien format du topic) : refuse, ce n'est pas de l'Avro")
    void json_isRejected() {
        byte[] json = "{\"eventId\":\"evt-1\"}".getBytes(StandardCharsets.UTF_8);

        assertThatThrownBy(() -> codec.decode(json)).isInstanceOf(BadHeaderException.class);
    }

    @Test
    @DisplayName("Octets quelconques : refuses")
    void garbage_isRejected() {
        assertThatThrownBy(() -> codec.decode(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}))
                .isInstanceOf(BadHeaderException.class);
    }

    @Test
    @DisplayName("Schema d'ecriture inconnu (version plus recente que ce consommateur) : refuse avec le motif")
    void unknownWriterSchema_isRejected() {
        byte[] bytes = codec.encode(event(null));
        bytes[5] ^= 0x7F;     // une autre empreinte : un schema que ce code n'a jamais vu

        assertThatThrownBy(() -> codec.decode(bytes)).isInstanceOf(MissingSchemaException.class);
    }

    @Test
    @DisplayName("Message tronque : refuse")
    void truncatedMessage_isRejected() {
        byte[] bytes = codec.encode(event(null));
        byte[] truncated = Arrays.copyOf(bytes, bytes.length - 8);

        assertThatThrownBy(() -> codec.decode(truncated))
                .isInstanceOfAny(IOException.class, AvroRuntimeException.class);
    }

    @Test
    @DisplayName("Montant : 39,8 est ecrit 39,80 ; une troisieme decimale est une erreur, pas un arrondi silencieux")
    void amounts_areWrittenWithTwoDecimals() throws IOException {
        OrderCreatedEvent shortScale = new OrderCreatedEvent(
                "evt-1", "ord-1", "cust-118", List.of(new OrderLine("sku-001", 1, new BigDecimal("39.8"))),
                new BigDecimal("39.8"), NOW);

        OrderCreatedEvent decoded = codec.decode(codec.encode(shortScale));

        assertThat(decoded.totalAmount()).isEqualByComparingTo("39.80");
        assertThat(decoded.totalAmount().scale()).isEqualTo(2);

        OrderCreatedEvent tooPrecise = new OrderCreatedEvent(
                "evt-2", "ord-2", "cust-118", List.of(new OrderLine("sku-001", 1, new BigDecimal("39.8"))),
                new BigDecimal("39.805"), NOW);
        assertThatThrownBy(() -> codec.encode(tooPrecise)).isInstanceOf(ArithmeticException.class);
    }
}
