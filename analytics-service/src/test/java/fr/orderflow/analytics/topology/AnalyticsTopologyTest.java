package fr.orderflow.analytics.topology;

import fr.orderflow.common.event.OrderCancelledEvent;
import fr.orderflow.common.event.OrderConfirmedEvent;
import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.event.PaymentCompletedEvent;
import fr.orderflow.common.messaging.Topics;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests de la topologie Kafka Streams sans broker : {@link TopologyTestDriver} execute la topologie dans
 * le thread du test, avec de vrais state stores (RocksDB).
 */
class AnalyticsTopologyTest {

    private static final Instant NOW = Instant.parse("2026-10-08T10:15:30Z");

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderCreatedEvent> created;
    private TestInputTopic<String, OrderConfirmedEvent> confirmed;
    private TestInputTopic<String, OrderCancelledEvent> cancelled;
    private TestInputTopic<String, PaymentCompletedEvent> payments;

    private static OrderCreatedEvent orderCreated(String orderId) {
        return new OrderCreatedEvent(
                "evt-created-" + orderId, orderId, "cust-118",
                List.of(new OrderLine("sku-001", 2, new BigDecimal("19.90"))), new BigDecimal("39.80"), NOW);
    }

    private static OrderConfirmedEvent orderConfirmed(String orderId) {
        return new OrderConfirmedEvent("evt-confirmed-" + orderId, orderId, NOW);
    }

    private static OrderCancelledEvent orderCancelled(String orderId) {
        return new OrderCancelledEvent("evt-cancelled-" + orderId, orderId, List.of(), "Plafond depasse", NOW);
    }

    private static PaymentCompletedEvent payment(String orderId, String amount) {
        return new PaymentCompletedEvent("evt-payment-" + orderId, orderId, new BigDecimal(amount), "tx-" + orderId, NOW);
    }

    @BeforeEach
    void setUp() {
        driver = newDriver();
        bindTopics(driver);
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private TopologyTestDriver newDriver() {
        StreamsBuilder builder = new StreamsBuilder();
        AnalyticsTopology.build(builder);
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "analytics-test-" + UUID.randomUUID());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.resolve(UUID.randomUUID().toString()).toString());
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                "org.apache.kafka.streams.errors.LogAndContinueExceptionHandler");
        props.put(StreamsConfig.ERRORS_DEAD_LETTER_QUEUE_TOPIC_NAME_CONFIG, "orderflow-analytics.DLT");
        return new TopologyTestDriver(builder.build(), props);
    }

    private void bindTopics(TopologyTestDriver d) {
        created = d.createInputTopic(Topics.ORDERS_CREATED, new StringSerializer(), EventSerdes.orderCreated().serializer());
        confirmed = d.createInputTopic(Topics.ORDERS_CONFIRMED, new StringSerializer(),
                EventSerdes.json(OrderConfirmedEvent.class).serializer());
        cancelled = d.createInputTopic(Topics.ORDERS_CANCELLED, new StringSerializer(),
                EventSerdes.json(OrderCancelledEvent.class).serializer());
        payments = d.createInputTopic(Topics.PAYMENTS_COMPLETED, new StringSerializer(),
                EventSerdes.json(PaymentCompletedEvent.class).serializer());
    }

    private static long count(TopologyTestDriver d, String status) {
        KeyValueStore<String, Long> store = d.getKeyValueStore(AnalyticsTopology.ORDERS_BY_STATUS);
        Long value = store.get(status);
        return value == null ? 0 : value;
    }

    private static BigDecimal revenue(TopologyTestDriver d) {
        KeyValueStore<String, BigDecimal> store = d.getKeyValueStore(AnalyticsTopology.REVENUE);
        BigDecimal value = store.get(AnalyticsTopology.REVENUE_KEY);
        return value == null ? BigDecimal.ZERO : value;
    }

    private long count(String status) {
        return count(driver, status);
    }

    @Test
    @DisplayName("Une commande creee compte pour CREATED")
    void createdOrder_isCountedAsCreated() {
        created.pipeInput("ord-1", orderCreated("ord-1"));

        assertThat(count("CREATED")).isEqualTo(1);
        assertThat(count("CONFIRMED")).isZero();
        assertThat(count("CANCELLED")).isZero();
    }

    @Test
    @DisplayName("Confirmee : la commande quitte CREATED pour CONFIRMED (statut courant, pas cumul d'evenements)")
    void confirmedOrder_movesFromCreatedToConfirmed() {
        created.pipeInput("ord-1", orderCreated("ord-1"));
        confirmed.pipeInput("ord-1", orderConfirmed("ord-1"));

        assertThat(count("CREATED")).isZero();
        assertThat(count("CONFIRMED")).isEqualTo(1);
    }

    @Test
    @DisplayName("Annulee : la commande quitte CREATED pour CANCELLED")
    void cancelledOrder_movesFromCreatedToCancelled() {
        created.pipeInput("ord-1", orderCreated("ord-1"));
        cancelled.pipeInput("ord-1", orderCancelled("ord-1"));

        assertThat(count("CREATED")).isZero();
        assertThat(count("CANCELLED")).isEqualTo(1);
    }

    @Test
    @DisplayName("Evenements en double (outbox at-least-once) : les compteurs n'augmentent pas")
    void duplicatedEvents_doNotInflateCounts() {
        created.pipeInput("ord-1", orderCreated("ord-1"));
        created.pipeInput("ord-1", orderCreated("ord-1"));
        confirmed.pipeInput("ord-1", orderConfirmed("ord-1"));
        confirmed.pipeInput("ord-1", orderConfirmed("ord-1"));
        payments.pipeInput("ord-1", payment("ord-1", "39.80"));
        payments.pipeInput("ord-1", payment("ord-1", "39.80"));

        assertThat(count("CREATED")).isZero();
        assertThat(count("CONFIRMED")).isEqualTo(1);
        assertThat(revenue(driver)).isEqualByComparingTo("39.80");
    }

    @Test
    @DisplayName("OrderConfirmed lu AVANT OrderCreated (trois topics, aucun ordre garanti) : la commande reste CONFIRMED")
    void confirmedBeforeCreated_stillEndsConfirmed() {
        confirmed.pipeInput("ord-1", orderConfirmed("ord-1"));
        created.pipeInput("ord-1", orderCreated("ord-1"));

        assertThat(count("CONFIRMED")).isEqualTo(1);
        assertThat(count("CREATED")).as("OrderCreated tardif ne fait pas redescendre la commande").isZero();
    }

    @Test
    @DisplayName("Chiffre d'affaires : somme des paiements acceptes, un paiement redelivre n'est compte qu'une fois")
    void revenue_sumsDistinctPayments() {
        payments.pipeInput("ord-1", payment("ord-1", "39.80"));
        payments.pipeInput("ord-2", payment("ord-2", "100.00"));
        payments.pipeInput("ord-2", payment("ord-2", "100.00"));

        assertThat(revenue(driver)).isEqualByComparingTo("139.80");
    }

    @Test
    @DisplayName("Rejeu depuis le debut : le resultat ne depend pas de l'ordre d'arrivee des evenements")
    void anyArrivalOrder_givesTheSameResult() {
        List<Runnable> log = new ArrayList<>();
        // o1 et o4 confirmees (paiements 39,80 et 100,00), o2 et o5 annulees, o3 encore en cours
        for (String o : List.of("o1", "o2", "o3", "o4", "o5")) {
            log.add(() -> created.pipeInput(o, orderCreated(o)));
        }
        for (String o : List.of("o1", "o4")) {
            log.add(() -> confirmed.pipeInput(o, orderConfirmed(o)));
            log.add(() -> payments.pipeInput(o, payment(o, o.equals("o1") ? "39.80" : "100.00")));
        }
        for (String o : List.of("o2", "o5")) {
            log.add(() -> cancelled.pipeInput(o, orderCancelled(o)));
        }

        for (long seed = 0; seed < 25; seed++) {
            List<Runnable> shuffled = new ArrayList<>(log);
            Collections.shuffle(shuffled, new Random(seed));
            try (TopologyTestDriver replay = newDriver()) {
                bindTopics(replay);
                shuffled.forEach(Runnable::run);

                assertThat(count(replay, "CREATED")).as("seed %d", seed).isEqualTo(1);
                assertThat(count(replay, "CONFIRMED")).as("seed %d", seed).isEqualTo(2);
                assertThat(count(replay, "CANCELLED")).as("seed %d", seed).isEqualTo(2);
                assertThat(revenue(replay)).as("seed %d", seed).isEqualByComparingTo("139.80");
            }
        }
    }

    @Test
    @DisplayName("Message illisible : l'application continue et le message part en DLQ, les agregats sont intacts")
    void poisonPill_isSkippedAndSentToDeadLetterQueue() {
        TestInputTopic<String, byte[]> raw = driver.createInputTopic(
                Topics.ORDERS_CONFIRMED, new StringSerializer(), new ByteArraySerializer());
        TestOutputTopic<String, String> dlq = driver.createOutputTopic(
                "orderflow-analytics.DLT", new StringDeserializer(), new StringDeserializer());
        created.pipeInput("ord-1", orderCreated("ord-1"));

        raw.pipeInput("ord-1", "ceci n'est pas du JSON".getBytes());
        confirmed.pipeInput("ord-1", orderConfirmed("ord-1"));     // le flux continue apres le message illisible

        assertThat(count("CONFIRMED")).isEqualTo(1);
        assertThat(dlq.getQueueSize()).as("message illisible envoye en DLQ").isEqualTo(1);
    }
}
