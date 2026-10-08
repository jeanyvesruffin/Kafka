package fr.orderflow.analytics.topology;

import fr.orderflow.common.event.OrderCancelledEvent;
import fr.orderflow.common.event.OrderConfirmedEvent;
import fr.orderflow.common.event.PaymentCompletedEvent;
import fr.orderflow.common.messaging.Topics;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;
import java.util.List;

/**
 * Topologie Kafka Streams du tableau de bord (phase 8, exigence EF-08).
 *
 * <pre>
 * orders.created   (Avro) ─┐
 * orders.confirmed (JSON) ─┼─ statut par commande ─ KTable[orderId -> statut] ─ groupBy(statut).count()
 * orders.cancelled (JSON) ─┘                                                      └─> store "orders-by-status"
 *
 * payments.completed (JSON) ── montant par commande ── KTable[orderId -> montant] ── somme
 *                                                                                    └─> store "revenue"
 * </pre>
 *
 * <h3>Pourquoi des KTables et pas de simples compteurs d'evenements</h3>
 * Un compteur incremente a chaque evenement compterait deux fois un evenement publie deux fois, ce qui
 * arrive : l'outbox livre <i>au moins une fois</i> (voir {@code OutboxRelay}). Ici l'etat de chaque
 * commande est une <b>valeur par cle</b> : un doublon la laisse inchangee. Les agregats sont derives de
 * ces valeurs (une mise a jour retire l'ancienne contribution avant d'ajouter la nouvelle), donc
 * <b>idempotents</b>.
 *
 * <h3>Ordre entre topics</h3>
 * {@code orders.created}, {@code orders.confirmed} et {@code orders.cancelled} sont trois topics : Kafka ne
 * garantit aucun ordre entre eux. Un {@code OrderConfirmed} peut etre lu avant l'{@code OrderCreated}
 * correspondant, surtout pendant un rejeu depuis le debut. Le statut retenu est donc le plus
 * <i>avance</i> ({@link #mostAdvanced}) : le resultat ne depend pas de l'ordre d'arrivee.
 *
 * <p>Les valeurs sont celles d'un instant donne : {@code CREATED} = commandes en cours, {@code CONFIRMED}
 * et {@code CANCELLED} = commandes terminees.
 */
public final class AnalyticsTopology {

    public static final String CREATED = "CREATED";
    public static final String CONFIRMED = "CONFIRMED";
    public static final String CANCELLED = "CANCELLED";
    public static final List<String> STATUSES = List.of(CREATED, CONFIRMED, CANCELLED);

    /**
     * State store : statut -> nombre de commandes.
     */
    public static final String ORDERS_BY_STATUS = "orders-by-status";
    /**
     * State store : cle fixe {@link #REVENUE_KEY} -> chiffre d'affaires cumule.
     */
    public static final String REVENUE = "revenue";
    public static final String REVENUE_KEY = "total";

    static final String STATUS_BY_ORDER = "status-by-order";
    static final String AMOUNT_BY_ORDER = "amount-by-order";

    private AnalyticsTopology() {
    }

    public static void build(StreamsBuilder builder) {
        ordersByStatus(builder);
        revenue(builder);
    }

    private static void ordersByStatus(StreamsBuilder builder) {
        KStream<String, String> created = builder
                .stream(Topics.ORDERS_CREATED, Consumed.with(Serdes.String(), EventSerdes.orderCreated()))
                .mapValues(event -> CREATED);
        KStream<String, String> confirmed = builder
                .stream(Topics.ORDERS_CONFIRMED,
                        Consumed.with(Serdes.String(), EventSerdes.json(OrderConfirmedEvent.class)))
                .mapValues(event -> CONFIRMED);
        KStream<String, String> cancelled = builder
                .stream(Topics.ORDERS_CANCELLED,
                        Consumed.with(Serdes.String(), EventSerdes.json(OrderCancelledEvent.class)))
                .mapValues(event -> CANCELLED);

        KTable<String, String> statusByOrder = created.merge(confirmed).merge(cancelled)
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .reduce(AnalyticsTopology::mostAdvanced,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(STATUS_BY_ORDER)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()));

        statusByOrder
                .groupBy((orderId, status) -> KeyValue.pair(status, orderId),
                        Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(ORDERS_BY_STATUS)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));
    }

    private static void revenue(StreamsBuilder builder) {
        KTable<String, BigDecimal> amountByOrder = builder
                .stream(Topics.PAYMENTS_COMPLETED,
                        Consumed.with(Serdes.String(), EventSerdes.json(PaymentCompletedEvent.class)))
                .mapValues(PaymentCompletedEvent::amount)
                .toTable(Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as(AMOUNT_BY_ORDER)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(EventSerdes.bigDecimal()));

        amountByOrder
                .groupBy((orderId, amount) -> KeyValue.pair(REVENUE_KEY, amount),
                        Grouped.with(Serdes.String(), EventSerdes.bigDecimal()))
                .aggregate(
                        () -> BigDecimal.ZERO,
                        (key, amount, total) -> total.add(amount),          // nouvelle contribution
                        (key, amount, total) -> total.subtract(amount),     // retrait de l'ancienne
                        Materialized.<String, BigDecimal, KeyValueStore<Bytes, byte[]>>as(REVENUE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(EventSerdes.bigDecimal()));
    }

    /**
     * Le plus avance des deux statuts : {@code CREATED < CONFIRMED < CANCELLED}. Une commande
     * ne repasse jamais a {@code CREATED}, et le resultat ne depend pas de l'ordre d'arrivee. Les deux
     * statuts terminaux sont exclusifs dans la saga ; le classement n'existe que pour rendre
     * deterministe le cas, anormal, ou les deux seraient presents.
     */
    static String mostAdvanced(String current, String candidate) {
        return rank(candidate) > rank(current) ? candidate : current;
    }

    private static int rank(String status) {
        return STATUSES.indexOf(status);
    }
}
