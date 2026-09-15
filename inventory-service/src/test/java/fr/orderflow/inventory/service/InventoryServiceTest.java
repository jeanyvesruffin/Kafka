package fr.orderflow.inventory.service;

import fr.orderflow.common.event.*;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventPublisher;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.inventory.domain.ProcessedEventEntity;
import fr.orderflow.inventory.domain.StockEntity;
import fr.orderflow.inventory.repository.ProcessedEventRepository;
import fr.orderflow.inventory.repository.StockRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

class InventoryServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-07T10:15:30Z");
    private static final String CID = "corr-test";

    private StockEntity stock;
    private List<EventEnvelope> published;
    private Set<String> processedIds;
    private InventoryService inventoryService;
    private EventSerializer eventSerializer;

    private static OrderCreatedEvent orderCreated(int quantity) {
        OrderLine line = line(quantity);
        return new OrderCreatedEvent("evt-1", "ord-1", "cust-118", List.of(line), line.lineTotal(), NOW);
    }

    private static OrderLine line(int quantity) {
        return new OrderLine("sku-001", quantity, new BigDecimal("19.90"));
    }

    @BeforeEach
    void setUp() {
        stock = new StockEntity("sku-001", 10);
        published = new ArrayList<>();
        processedIds = new HashSet<>();

        var stockRepository = Mockito.mock(StockRepository.class);
        Mockito.when(stockRepository.findById("sku-001"))
                .thenReturn(Optional.of(stock));
        Mockito.when(stockRepository.findById("sku-inconnu"))
                .thenReturn(Optional.empty());

        var processedRepository = Mockito.mock(ProcessedEventRepository.class);
        Mockito.when(processedRepository.existsById(Mockito.anyString()))
                .thenAnswer(inv -> processedIds.contains(inv.getArgument(0, String.class)));
        Mockito.when(processedRepository.save(Mockito.any()))
                .thenAnswer(inv -> {
                    processedIds.add(inv.getArgument(0, ProcessedEventEntity.class)
                            .getEventId());
                    return inv.getArgument(0);
                });

        EventPublisher publisher = published::add;
        eventSerializer = new EventSerializer(JsonMapper.builder()
                .build());
        inventoryService = new InventoryService(
                stockRepository, processedRepository,
                publisher, eventSerializer, Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Stock suffisant : reservation effectuee et InventoryReserved publie")
    void enoughStock_reserves() {
        inventoryService.handleOrderCreated(orderCreated(3), CID);

        assertThat(stock.getQuantityAvailable()).isEqualTo(7);
        assertThat(stock.getQuantityReserved()).isEqualTo(3);
        assertThat(published).hasSize(1);
        assertThat(published.getFirst()
                .topic()).isEqualTo(Topics.INVENTORY_RESERVED);
        assertThat(eventSerializer.fromJson(
                        published.getFirst()
                                .payload(), InventoryReservedEvent.class)
                .orderId()).isEqualTo("ord-1");
    }

    @Test
    @DisplayName("Stock insuffisant : rien n'est reserve et InventoryRejected est publie")
    void notEnoughStock_rejects() {
        inventoryService.handleOrderCreated(orderCreated(999), CID);

        assertThat(stock.getQuantityAvailable()).isEqualTo(10);
        assertThat(stock.getQuantityReserved()).isZero();
        assertThat(published).hasSize(1);
        assertThat(published.getFirst()
                .topic()).isEqualTo(Topics.INVENTORY_REJECTED);
        assertThat(eventSerializer.fromJson(
                        published.getFirst()
                                .payload(), InventoryRejectedEvent.class)
                .reason()).contains("sku-001");
    }

    @Test
    @DisplayName("Le meme evenement redelivre ne reserve pas le stock deux fois (idempotence)")
    void redelivery_doesNotReserveTwice() {
        var event = orderCreated(3);

        inventoryService.handleOrderCreated(event, CID);
        inventoryService.handleOrderCreated(event, CID);   // redelivery apres rebalance

        assertThat(stock.getQuantityAvailable()).isEqualTo(7);
        assertThat(published).hasSize(1);
    }

    // ------------------------------------------------------------------

    @Test
    @DisplayName("OrderCancelled libere le stock precedemment reserve (compensation)")
    void cancellation_releasesStock() {
        inventoryService.handleOrderCreated(orderCreated(3), CID);
        assertThat(stock.getQuantityAvailable()).isEqualTo(7);

        inventoryService.handleOrderCancelled(new OrderCancelledEvent(
                "evt-2", "ord-1", List.of(line(3)), "Paiement refuse", NOW));

        assertThat(stock.getQuantityAvailable()).isEqualTo(10);
        assertThat(stock.getQuantityReserved()).isZero();
    }

    @Test
    @DisplayName("La compensation est sure meme si aucune reservation n'avait eu lieu")
    void cancellationWithoutReservation_isSafe() {
        inventoryService.handleOrderCancelled(new OrderCancelledEvent(
                "evt-2", "ord-1", List.of(line(3)), "Stock insuffisant", NOW));

        assertThat(stock.getQuantityAvailable()).isEqualTo(10);
        assertThat(stock.getQuantityReserved()).isZero();
    }
}
