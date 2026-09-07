package fr.orderflow.inventory.service;

import static org.assertj.core.api.Assertions.assertThat;

import fr.orderflow.common.event.InventoryRejectedEvent;
import fr.orderflow.common.event.InventoryReservedEvent;
import fr.orderflow.common.event.OrderCancelledEvent;
import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventPublisher;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.inventory.domain.ProcessedEventEntity;
import fr.orderflow.inventory.domain.StockEntity;
import fr.orderflow.inventory.repository.ProcessedEventRepository;
import fr.orderflow.inventory.repository.StockRepository;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import tools.jackson.databind.json.JsonMapper;

class InventoryServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-07T10:15:30Z");
    private static final String CID = "corr-test";

    private StockEntity stock;
    private List<EventEnvelope> published;
    private Set<String> processedIds;
    private InventoryService inventoryService;
    private EventSerializer eventSerializer;

    @BeforeEach
    void setUp() {
        stock = new StockEntity("sku-001", 10);
        published = new ArrayList<>();
        processedIds = new HashSet<>();

        var stockRepository = Mockito.mock(StockRepository.class);
        Mockito.when(stockRepository.findById("sku-001")).thenReturn(Optional.of(stock));
        Mockito.when(stockRepository.findById("sku-inconnu")).thenReturn(Optional.empty());

        var processedRepository = Mockito.mock(ProcessedEventRepository.class);
        Mockito.when(processedRepository.existsById(Mockito.anyString()))
                .thenAnswer(inv -> processedIds.contains(inv.getArgument(0, String.class)));
        Mockito.when(processedRepository.save(Mockito.any())).thenAnswer(inv -> {
            processedIds.add(inv.getArgument(0, ProcessedEventEntity.class).getEventId());
            return inv.getArgument(0);
        });

        EventPublisher publisher = published::add;
        eventSerializer = new EventSerializer(JsonMapper.builder().build());
        inventoryService = new InventoryService(stockRepository, processedRepository,
                publisher, eventSerializer, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Stock suffisant : reservation effectuee et InventoryReserved publie")
    void enoughStock_reserves() {
        inventoryService.handleOrderCreated(orderCreated("evt-1", "sku-001", 3), CID);

        assertThat(stock.getQuantityAvailable()).isEqualTo(7);
        assertThat(stock.getQuantityReserved()).isEqualTo(3);
        assertThat(published).hasSize(1);
        assertThat(published.getFirst().topic()).isEqualTo(Topics.INVENTORY_RESERVED);
        assertThat(eventSerializer.fromJson(published.getFirst().payload(), InventoryReservedEvent.class)
                .orderId()).isEqualTo("ord-1");
    }

    @Test
    @DisplayName("Stock insuffisant : rien n'est reserve et InventoryRejected est publie")
    void notEnoughStock_rejects() {
        inventoryService.handleOrderCreated(orderCreated("evt-1", "sku-001", 999), CID);

        assertThat(stock.getQuantityAvailable()).isEqualTo(10);
        assertThat(stock.getQuantityReserved()).isZero();
        assertThat(published).hasSize(1);
        assertThat(published.getFirst().topic()).isEqualTo(Topics.INVENTORY_REJECTED);
        assertThat(eventSerializer.fromJson(published.getFirst().payload(), InventoryRejectedEvent.class)
                .reason()).contains("sku-001");
    }

    @Test
    @DisplayName("Le meme evenement redelivre ne reserve pas le stock deux fois (idempotence)")
    void redelivery_doesNotReserveTwice() {
        var event = orderCreated("evt-1", "sku-001", 3);

        inventoryService.handleOrderCreated(event, CID);
        inventoryService.handleOrderCreated(event, CID);   // redelivery apres rebalance

        assertThat(stock.getQuantityAvailable()).isEqualTo(7);
        assertThat(published).hasSize(1);
    }

    @Test
    @DisplayName("OrderCancelled libere le stock precedemment reserve (compensation)")
    void cancellation_releasesStock() {
        inventoryService.handleOrderCreated(orderCreated("evt-1", "sku-001", 3), CID);
        assertThat(stock.getQuantityAvailable()).isEqualTo(7);

        inventoryService.handleOrderCancelled(new OrderCancelledEvent(
                "evt-2", "ord-1", List.of(line("sku-001", 3)), "Paiement refuse", NOW));

        assertThat(stock.getQuantityAvailable()).isEqualTo(10);
        assertThat(stock.getQuantityReserved()).isZero();
    }

    @Test
    @DisplayName("La compensation est sure meme si aucune reservation n'avait eu lieu")
    void cancellationWithoutReservation_isSafe() {
        inventoryService.handleOrderCancelled(new OrderCancelledEvent(
                "evt-2", "ord-1", List.of(line("sku-001", 3)), "Stock insuffisant", NOW));

        assertThat(stock.getQuantityAvailable()).isEqualTo(10);
        assertThat(stock.getQuantityReserved()).isZero();
    }

    // ------------------------------------------------------------------

    private static OrderCreatedEvent orderCreated(String eventId, String productId, int quantity) {
        OrderLine line = line(productId, quantity);
        return new OrderCreatedEvent(eventId, "ord-1", "cust-118", List.of(line), line.lineTotal(), NOW);
    }

    private static OrderLine line(String productId, int quantity) {
        return new OrderLine(productId, quantity, new BigDecimal("19.90"));
    }
}
