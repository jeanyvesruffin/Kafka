package fr.orderflow.order.service;

import fr.orderflow.common.event.*;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.order.api.CreateOrderRequest;
import fr.orderflow.order.domain.OrderEntity;
import fr.orderflow.order.domain.OrderItemEntity;
import fr.orderflow.order.domain.OrderStatus;
import fr.orderflow.order.domain.OutboxEventEntity;
import fr.orderflow.order.repository.OrderRepository;
import fr.orderflow.order.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Source de verite de la commande.
 *
 * <p>Chaque methode publique qui fait evoluer la commande ecrit, DANS LA MEME
 * TRANSACTION, la modification metier et la ligne d'outbox correspondante.
 * C'est ce qui rend impossible la divergence entre l'etat en base et les
 * evenements emis.
 *
 * <p><b>Les methodes {@code onXxx} sont les points d'entree de tes futurs
 * listeners Kafka.</b> Elles sont deja ecrites, testees et idempotentes : ton
 * listener n'aura qu'a deserialiser l'evenement et appeler la bonne methode.
 * Aucune logique metier n'est a ecrire dans le listener.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orderRepository;
    private final OutboxEventRepository outboxRepository;
    private final PriceCatalog priceCatalog;
    private final EventSerializer eventSerializer;
    private final Clock clock;

    public OrderService(OrderRepository orderRepository,
                        OutboxEventRepository outboxRepository,
                        PriceCatalog priceCatalog,
                        EventSerializer eventSerializer,
                        Clock clock) {
        this.orderRepository = orderRepository;
        this.outboxRepository = outboxRepository;
        this.priceCatalog = priceCatalog;
        this.eventSerializer = eventSerializer;
        this.clock = clock;
    }

    // ------------------------------------------------------------------
    // Commande entrante (API REST)
    // ------------------------------------------------------------------

    private static String newEventId() {
        return UUID.randomUUID()
                .toString();
    }

    // ------------------------------------------------------------------
    // Reactions aux evenements des autres services
    // (a appeler depuis tes futurs @KafkaListener)
    // ------------------------------------------------------------------

    @Transactional
    public OrderEntity createOrder(CreateOrderRequest request, String correlationId) {
        Instant now = clock.instant();
        String orderId = "ord-" + UUID.randomUUID()
                .toString()
                .substring(0, 8);

        List<OrderLine> lines = request.items()
                .stream()
                .map(i -> new OrderLine(i.productId(), i.quantity(), priceCatalog.priceOf(i.productId())))
                .toList();

        BigDecimal total = lines.stream()
                .map(OrderLine::lineTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        OrderEntity order = new OrderEntity(orderId, request.customerId(), total, now);
        lines.forEach(line -> order.addItem(new OrderItemEntity(
                UUID.randomUUID()
                        .toString(), line.productId(), line.quantity(), line.unitPrice())));
        orderRepository.save(order);

        appendToOutbox(
                Topics.ORDERS_CREATED, new OrderCreatedEvent(
                        newEventId(), orderId, request.customerId(), lines, total, now), correlationId);

        log.info("Commande creee orderId={} total={} correlationId={}", orderId, total, correlationId);
        return order;
    }

    /**
     * Stock reserve : la commande avance, aucun evenement emis (Payment ecoute deja inventory.reserved).
     */
    @Transactional
    public void onInventoryReserved(String orderId, String correlationId) {
        OrderEntity order = load(orderId);
        if (order.transitionTo(OrderStatus.INVENTORY_RESERVED, null, clock.instant())) {
            log.info("Stock reserve orderId={} correlationId={}", orderId, correlationId);
        } else {
            log.debug("Transition ignoree (deja appliquee ou etat terminal) orderId={}", orderId);
        }
    }

    /**
     * Stock insuffisant : annulation. Rien a compenser, aucune reservation n'a eu lieu.
     */
    @Transactional
    public void onInventoryRejected(String orderId, String reason, String correlationId) {
        cancel(orderId, reason, correlationId);
    }

    /**
     * Paiement accepte : la commande est confirmee.
     */
    @Transactional
    public void onPaymentCompleted(String orderId, String correlationId) {
        OrderEntity order = load(orderId);
        Instant now = clock.instant();
        if (!order.transitionTo(OrderStatus.CONFIRMED, null, now)) {
            log.debug("PaymentCompleted ignore (deja traite) orderId={}", orderId);
            return;
        }
        appendToOutbox(
                Topics.ORDERS_CONFIRMED,
                new OrderConfirmedEvent(newEventId(), orderId, now), correlationId);
        log.info("Commande confirmee orderId={} correlationId={}", orderId, correlationId);
    }

    // ------------------------------------------------------------------
    // Lecture
    // ------------------------------------------------------------------

    /**
     * Paiement refuse : annulation + emission de {@code OrderCancelled}.
     *
     * <p>Cet evenement porte les lignes de commande, car c'est lui qui declenche
     * la COMPENSATION cote Inventory (liberation du stock deja reserve).
     */
    @Transactional
    public void onPaymentFailed(String orderId, String reason, String correlationId) {
        cancel(orderId, reason, correlationId);
    }

    @Transactional(readOnly = true)
    public OrderEntity findById(String orderId) {
        return load(orderId);
    }

    // ------------------------------------------------------------------
    // Interne
    // ------------------------------------------------------------------

    @Transactional(readOnly = true)
    public List<OrderEntity> findAll(OrderStatus status) {
        return status == null ? orderRepository.findAll() : orderRepository.findByStatus(status);
    }

    private void cancel(String orderId, String reason, String correlationId) {
        OrderEntity order = load(orderId);
        Instant now = clock.instant();
        if (!order.transitionTo(OrderStatus.CANCELLED, reason, now)) {
            log.debug("Annulation ignoree (deja terminal) orderId={}", orderId);
            return;
        }
        List<OrderLine> lines = order.getItems()
                .stream()
                .map(i -> new OrderLine(i.getProductId(), i.getQuantity(), i.getUnitPrice()))
                .toList();
        appendToOutbox(
                Topics.ORDERS_CANCELLED,
                new OrderCancelledEvent(newEventId(), orderId, lines, reason, now), correlationId);
        log.info("Commande annulee orderId={} raison={} correlationId={}", orderId, reason, correlationId);
    }

    /**
     * Ecrit l'evenement dans l'outbox. Appelee TOUJOURS depuis une methode
     * {@code @Transactional} : c'est ce qui rend l'ecriture atomique avec la
     * modification metier.
     */
    private void appendToOutbox(String topic, OrderFlowEvent event, String correlationId) {
        outboxRepository.save(new OutboxEventEntity(
                UUID.randomUUID()
                        .toString(),
                event.orderId(),
                event.eventId(),
                event.eventType(),
                topic,
                eventSerializer.toJson(event),
                correlationId,
                event.occurredAt()));
    }

    private OrderEntity load(String orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new OrderNotFoundException(orderId));
    }
}
