package fr.orderflow.order.service;

import static org.assertj.core.api.Assertions.assertThat;

import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.order.api.CreateOrderRequest;
import fr.orderflow.order.domain.OrderEntity;
import fr.orderflow.order.domain.OrderStatus;
import fr.orderflow.order.domain.OutboxEventEntity;
import fr.orderflow.order.repository.OrderRepository;
import fr.orderflow.order.repository.OutboxEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import tools.jackson.databind.json.JsonMapper;

/**
 * Tests unitaires purs — aucun contexte Spring, aucune base, aucun broker.
 *
 * <p>Ils tournent en quelques millisecondes et n'ont besoin d'aucune
 * infrastructure : c'est ce qui rend le projet compatible avec un poste sans
 * droits administrateur.
 */
class OrderServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-07T10:15:30Z");
    private static final String CID = "corr-test";

    private OrderRepository orderRepository;
    private OutboxEventRepository outboxRepository;
    private OrderService orderService;
    private EventSerializer eventSerializer;

    @BeforeEach
    void setUp() {
        orderRepository = Mockito.mock(OrderRepository.class);
        outboxRepository = Mockito.mock(OutboxEventRepository.class);
        eventSerializer = new EventSerializer(JsonMapper.builder().build());
        orderService = new OrderService(
                orderRepository,
                outboxRepository,
                new PriceCatalog(),
                eventSerializer,
                Clock.fixed(NOW, ZoneOffset.UTC));

        Mockito.when(orderRepository.save(Mockito.any())).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    @DisplayName("Creer une commande ecrit la commande ET la ligne d'outbox dans la meme transaction")
    void createOrder_writesOrderAndOutbox() {
        var request = new CreateOrderRequest("cust-118",
                List.of(new CreateOrderRequest.Item("sku-001", 2)));

        OrderEntity order = orderService.createOrder(request, CID);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CREATED);
        assertThat(order.getTotalAmount()).isEqualByComparingTo("39.80");
        assertThat(order.getItems()).hasSize(1);

        OutboxEventEntity outbox = captureOutbox();
        assertThat(outbox.getTopic()).isEqualTo(Topics.ORDERS_CREATED);
        assertThat(outbox.getEventType()).isEqualTo(OrderCreatedEvent.TYPE);
        assertThat(outbox.getAggregateId()).isEqualTo(order.getId());
        assertThat(outbox.getCorrelationId()).isEqualTo(CID);
        assertThat(outbox.isPublished()).isFalse();

        // Le payload doit etre relisable : c'est le contrat avec les consommateurs.
        OrderCreatedEvent event = eventSerializer.fromJson(outbox.getPayload(), OrderCreatedEvent.class);
        assertThat(event.customerId()).isEqualTo("cust-118");
        assertThat(event.totalAmount()).isEqualByComparingTo("39.80");
        assertThat(event.occurredAt()).isEqualTo(NOW);
    }

    @Test
    @DisplayName("Un paiement refuse annule la commande et emet OrderCancelled avec les lignes a compenser")
    void onPaymentFailed_cancelsAndEmitsCompensationEvent() {
        OrderEntity order = existingOrder(OrderStatus.INVENTORY_RESERVED);

        orderService.onPaymentFailed(order.getId(), "Plafond depasse", CID);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        assertThat(order.getCancellationReason()).isEqualTo("Plafond depasse");

        OutboxEventEntity outbox = captureOutbox();
        assertThat(outbox.getTopic()).isEqualTo(Topics.ORDERS_CANCELLED);
        // Les lignes DOIVENT etre presentes : sans elles, Inventory ne peut pas compenser.
        assertThat(outbox.getPayload()).contains("sku-001");
    }

    @Test
    @DisplayName("Rejouer le meme evenement ne produit aucun effet la seconde fois (idempotence)")
    void replayingSameEvent_isIdempotent() {
        OrderEntity order = existingOrder(OrderStatus.INVENTORY_RESERVED);

        orderService.onPaymentCompleted(order.getId(), CID);
        orderService.onPaymentCompleted(order.getId(), CID);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CONFIRMED);
        // Une seule ligne d'outbox, pas deux : le second appel a ete absorbe.
        Mockito.verify(outboxRepository, Mockito.times(1)).save(Mockito.any());
    }

    @Test
    @DisplayName("Une commande deja annulee ne peut pas etre confirmee (etat terminal)")
    void terminalState_cannotBeLeft() {
        OrderEntity order = existingOrder(OrderStatus.CANCELLED);

        orderService.onPaymentCompleted(order.getId(), CID);

        assertThat(order.getStatus()).isEqualTo(OrderStatus.CANCELLED);
        Mockito.verify(outboxRepository, Mockito.never()).save(Mockito.any());
    }

    // ------------------------------------------------------------------

    private OrderEntity existingOrder(OrderStatus status) {
        var request = new CreateOrderRequest("cust-118",
                List.of(new CreateOrderRequest.Item("sku-001", 2)));
        OrderEntity order = orderService.createOrder(request, CID);
        order.transitionTo(status, null, NOW);
        Mockito.reset(outboxRepository);
        Mockito.when(orderRepository.findById(order.getId())).thenReturn(Optional.of(order));
        return order;
    }

    private OutboxEventEntity captureOutbox() {
        var captor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        Mockito.verify(outboxRepository).save(captor.capture());
        return captor.getValue();
    }
}
