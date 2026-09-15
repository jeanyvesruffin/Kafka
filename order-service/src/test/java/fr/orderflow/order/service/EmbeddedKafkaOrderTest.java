package fr.orderflow.order.service;

import fr.orderflow.common.event.*;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventHeaders;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.order.api.CreateOrderRequest;
import fr.orderflow.order.api.OrderResponse;
import fr.orderflow.order.domain.OrderEntity;
import fr.orderflow.order.domain.OrderStatus;
import fr.orderflow.order.domain.OutboxEventEntity;
import fr.orderflow.order.repository.OutboxEventRepository;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.annotation.DirtiesContext;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

/**
 * Tests d'integration Kafka du service Commande, sur un broker embarque.
 *
 * <p>Deux sens sont couverts :
 * <ul>
 *   <li>entrant : le test joue Inventory et Payment et publie sur leurs topics,
 *       les listeners doivent faire avancer la commande ;</li>
 *   <li>sortant : le relais d'outbox (desactive en test, voir
 *       {@code orderflow.outbox.poll-interval-ms}) est declenche a la main et
 *       le test lit ce qui arrive sur {@code orders.*}.</li>
 * </ul>
 */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = {
        Topics.ORDERS_CREATED, Topics.ORDERS_CONFIRMED, Topics.ORDERS_CANCELLED,
        Topics.INVENTORY_RESERVED, Topics.INVENTORY_REJECTED,
        Topics.PAYMENTS_COMPLETED, Topics.PAYMENTS_FAILED})
@DirtiesContext
class EmbeddedKafkaOrderTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final String CID = "corr-embedded";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;
    @Value("${spring.embedded.kafka.brokers}")
    private String brokers;
    @Autowired
    private EventSerializer eventSerializer;
    @Autowired
    private OrderService orderService;
    @Autowired
    private OutboxRelay outboxRelay;
    @Autowired
    private OutboxEventRepository outboxRepository;

    private static String newEventId() {
        return UUID.randomUUID()
                .toString();
    }

    private static List<OrderLine> lines() {
        return List.of(new OrderLine("sku-001", 2, new BigDecimal("19.90")));
    }

    private static String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers()
                .lastHeader(name)
                .value(), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("Le relais d'outbox publie OrderCreated sur orders.created (cle = orderId, en-tetes compris)")
    void outboxRelay_publishesOrderCreated() {
        OrderEntity order = newOrder();

        outboxRelay.publishPending();

        ConsumerRecord<String, String> record = awaitRecord(Topics.ORDERS_CREATED, order.getId());
        assertThat(header(record, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        assertThat(header(record, EventHeaders.EVENT_TYPE)).isEqualTo(OrderCreatedEvent.TYPE);
        OrderCreatedEvent event = eventSerializer.fromJson(record.value(), OrderCreatedEvent.class);
        assertThat(event.customerId()).isEqualTo("cust-118");
        assertThat(event.totalAmount()).isEqualByComparingTo("39.80");
        assertThat(outboxRows(order.getId(), OrderCreatedEvent.TYPE))
                .extracting(OutboxEventEntity::isPublished)
                .containsExactly(true);
    }

    @Test
    @DisplayName("InventoryReserved fait passer la commande a INVENTORY_RESERVED")
    void inventoryReserved_movesOrderToInventoryReserved() {
        OrderEntity order = newOrder();

        publish(Topics.INVENTORY_RESERVED, new InventoryReservedEvent(
                newEventId(), order.getId(), "cust-118", lines(), order.getTotalAmount(), Instant.now()));

        awaitStatus(order.getId(), OrderStatus.INVENTORY_RESERVED);
    }

    @Test
    @DisplayName("InventoryRejected annule la commande avec la raison du rejet")
    void inventoryRejected_cancelsOrder() {
        OrderEntity order = newOrder();

        publish(Topics.INVENTORY_REJECTED, new InventoryRejectedEvent(
                newEventId(), order.getId(), "Stock insuffisant pour le produit sku-001", Instant.now()));

        awaitStatus(order.getId(), OrderStatus.CANCELLED);
        assertThat(orderService.findById(order.getId())
                .getCancellationReason()).isEqualTo("Stock insuffisant pour le produit sku-001");
    }

    @Test
    @DisplayName("Une commande relue hors transaction expose ses lignes (GET /api/orders et /api/orders/{id})")
    void readOrders_exposeItemsOutsideTransaction() {
        OrderEntity order = newOrder();

        OrderResponse byId = OrderResponse.from(orderService.findById(order.getId()));
        List<OrderResponse> byStatus = orderService.findAll(OrderStatus.CREATED)
                .stream()
                .map(OrderResponse::from)
                .toList();

        assertThat(byId.items()).extracting(OrderResponse.Line::productId, OrderResponse.Line::quantity)
                .containsExactly(tuple("sku-001", 2));
        assertThat(byStatus).extracting(OrderResponse::orderId)
                .contains(order.getId());
    }

    @Test
    @DisplayName("PaymentCompleted confirme la commande et OrderConfirmed part sur orders.confirmed")
    void paymentCompleted_confirmsOrderAndPublishesOrderConfirmed() {
        OrderEntity order = reservedOrder();

        publish(Topics.PAYMENTS_COMPLETED, new PaymentCompletedEvent(
                newEventId(), order.getId(), order.getTotalAmount(), "tx-42", Instant.now()));

        awaitStatus(order.getId(), OrderStatus.CONFIRMED);
        outboxRelay.publishPending();
        ConsumerRecord<String, String> record = awaitRecord(Topics.ORDERS_CONFIRMED, order.getId());
        // Le correlationId entre par l'en-tete Kafka et ressort par l'outbox.
        assertThat(header(record, EventHeaders.CORRELATION_ID)).isEqualTo(CID);
        assertThat(header(record, EventHeaders.EVENT_TYPE)).isEqualTo(OrderConfirmedEvent.TYPE);
    }

    @Test
    @DisplayName("PaymentFailed annule la commande et OrderCancelled porte les lignes a compenser")
    void paymentFailed_cancelsOrderAndPublishesCompensation() {
        OrderEntity order = reservedOrder();

        publish(Topics.PAYMENTS_FAILED, new PaymentFailedEvent(
                newEventId(), order.getId(), order.getTotalAmount(), "Plafond depasse", Instant.now()));

        awaitStatus(order.getId(), OrderStatus.CANCELLED);
        outboxRelay.publishPending();
        OrderCancelledEvent cancelled = eventSerializer.fromJson(
                awaitRecord(Topics.ORDERS_CANCELLED, order.getId()).value(), OrderCancelledEvent.class);
        assertThat(cancelled.reason()).isEqualTo("Plafond depasse");
        // Sans les lignes, Inventory ne pourrait pas liberer le stock.
        assertThat(cancelled.items()).extracting(OrderLine::productId, OrderLine::quantity)
                .containsExactly(tuple("sku-001", 2));
    }

    @Test
    @DisplayName("Un PaymentCompleted redelivre ne produit qu'un seul OrderConfirmed (idempotence)")
    void redeliveredPaymentCompleted_isIdempotent() {
        OrderEntity order = reservedOrder();
        var event = new PaymentCompletedEvent(
                newEventId(), order.getId(), order.getTotalAmount(), "tx-42", Instant.now());

        publish(Topics.PAYMENTS_COMPLETED, event);
        publish(Topics.PAYMENTS_COMPLETED, event);   // redelivery apres rebalance

        awaitStatus(order.getId(), OrderStatus.CONFIRMED);
        await().during(Duration.ofSeconds(3))
                .atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(outboxRows(order.getId(), OrderConfirmedEvent.TYPE)).hasSize(1));
    }

    // ------------------------------------------------------------------

    private OrderEntity newOrder() {
        return orderService.createOrder(new CreateOrderRequest(
                "cust-118", List.of(new CreateOrderRequest.Item("sku-001", 2))), CID);
    }

    private OrderEntity reservedOrder() {
        OrderEntity order = newOrder();
        orderService.onInventoryReserved(order.getId(), CID);
        return order;
    }

    private void awaitStatus(String orderId, OrderStatus expected) {
        await().atMost(TIMEOUT)
                .untilAsserted(() -> assertThat(orderService.findById(orderId)
                        .getStatus()).isEqualTo(expected));
    }

    private List<OutboxEventEntity> outboxRows(String orderId, String eventType) {
        return outboxRepository.findAll()
                .stream()
                .filter(row -> row.getAggregateId()
                        .equals(orderId) && row.getEventType()
                        .equals(eventType))
                .toList();
    }

    /**
     * Publie l'evenement tel que le ferait le service emetteur : cle = orderId, en-tetes standards.
     */
    private void publish(String topic, OrderFlowEvent event) {
        EventEnvelope envelope = eventSerializer.envelope(topic, event, CID);
        var record = new ProducerRecord<>(envelope.topic(), envelope.key(), envelope.payload());
        envelope.headers()
                .forEach((key, value) -> record.headers()
                        .add(key, value.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(record)
                .join();
    }

    private ConsumerRecord<String, String> awaitRecord(String topic, String key) {
        return await().atMost(TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> recordsFor(topic, key), records -> !records.isEmpty())
                .getFirst();
    }

    /**
     * Tous les messages du topic portant cette cle, lus du debut jusqu'a la fin.
     *
     * <p>{@code assign} plutot que {@code subscribe} : pas de consumer group,
     * donc pas d'attente de rebalance, et une lecture qui s'arrete au dernier offset.
     */
    private List<ConsumerRecord<String, String>> recordsFor(String topic, String key) {
        Map<String, Object> props = KafkaTestUtils.consumerProps(brokers, "embedded-test", false);
        try (Consumer<String, String> consumer =
                     new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(topic)
                    .stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

            List<ConsumerRecord<String, String>> records = new ArrayList<>();
            while (partitions.stream()
                    .anyMatch(tp -> consumer.position(tp) < endOffsets.get(tp))) {
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
