package fr.orderflow.inventory.service;

import fr.orderflow.common.event.InventoryRejectedEvent;
import fr.orderflow.common.event.InventoryReservedEvent;
import fr.orderflow.common.event.OrderCancelledEvent;
import fr.orderflow.common.event.OrderCreatedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.EventPublisher;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.inventory.domain.ProcessedEventEntity;
import fr.orderflow.inventory.domain.StockEntity;
import fr.orderflow.inventory.repository.ProcessedEventRepository;
import fr.orderflow.inventory.repository.StockRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Gestion du stock : reservation, rejet, compensation.
 *
 * <p>Les deux methodes publiques sont idempotentes par construction : elles
 * commencent par consulter le journal {@code processed_events}. Elles sont les
 * points d'entree de tes futurs listeners.
 *
 * <p>Contrairement au service Commande, celui-ci publie directement via
 * {@link EventPublisher} plutot que par une outbox. C'est un choix assume pour
 * le cas d'ecole : tu as ainsi les deux approches sous les yeux dans le meme
 * projet et tu peux comparer leurs garanties. Migrer Inventory vers une outbox
 * est d'ailleurs un bon exercice supplementaire.
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final StockRepository stockRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final EventPublisher eventPublisher;
    private final EventSerializer eventSerializer;
    private final Clock clock;

    public InventoryService(StockRepository stockRepository,
                            ProcessedEventRepository processedEventRepository,
                            EventPublisher eventPublisher,
                            EventSerializer eventSerializer,
                            Clock clock) {
        this.stockRepository = stockRepository;
        this.processedEventRepository = processedEventRepository;
        this.eventPublisher = eventPublisher;
        this.eventSerializer = eventSerializer;
        this.clock = clock;
    }

    /**
     * Reagit a {@code OrderCreated} : tente de reserver le stock de toutes les
     * lignes. Tout ou rien — une seule ligne indisponible fait echouer la
     * reservation complete et emet {@code InventoryRejected}.
     */
    @Transactional
    public void handleOrderCreated(OrderCreatedEvent event, String correlationId) {
        if (alreadyProcessed(event.eventId())) {
            log.debug("Evenement deja traite, ignore eventId={}", event.eventId());
            return;
        }
        Instant now = clock.instant();
        markProcessed(event.eventId(), event.eventType(), now);

        Optional<String> unavailable = firstUnavailableProduct(event.items());

        if (unavailable.isPresent()) {
            String reason = "Stock insuffisant pour le produit " + unavailable.get();
            log.info("Reservation refusee orderId={} raison={}", event.orderId(), reason);
            publish(Topics.INVENTORY_REJECTED,
                    new InventoryRejectedEvent(newEventId(), event.orderId(), reason, now),
                    correlationId);
            return;
        }

        for (OrderLine line : event.items()) {
            stockRepository.findById(line.productId())
                    .orElseThrow(() -> new IllegalStateException("Produit inconnu : " + line.productId()))
                    .reserve(line.quantity());
        }

        log.info("Stock reserve orderId={} lignes={}", event.orderId(), event.items().size());
        publish(Topics.INVENTORY_RESERVED,
                new InventoryReservedEvent(newEventId(), event.orderId(), event.customerId(),
                        event.items(), event.totalAmount(), now),
                correlationId);
    }

    /**
     * Reagit a {@code OrderCancelled} : COMPENSATION.
     *
     * <p>C'est le mecanisme qui remplace le rollback d'une transaction
     * distribuee. La reservation n'est pas annulee, elle est compensee par une
     * operation inverse.
     *
     * <p>Robuste par nature : si la commande a ete annulee avant toute
     * reservation (rejet stock), {@code release} ne trouve rien a liberer et ne
     * fait rien de mal.
     */
    @Transactional
    public void handleOrderCancelled(OrderCancelledEvent event) {
        if (alreadyProcessed(event.eventId())) {
            log.debug("Compensation deja appliquee, ignoree eventId={}", event.eventId());
            return;
        }
        markProcessed(event.eventId(), event.eventType(), clock.instant());

        for (OrderLine line : event.items()) {
            stockRepository.findById(line.productId())
                    .ifPresent(stock -> stock.release(line.quantity()));
        }
        log.info("Stock libere (compensation) orderId={} raison={}", event.orderId(), event.reason());
    }

    @Transactional(readOnly = true)
    public List<StockEntity> findAllStock() {
        return stockRepository.findAll();
    }

    // ------------------------------------------------------------------

    private Optional<String> firstUnavailableProduct(List<OrderLine> items) {
        List<String> problems = new ArrayList<>();
        for (OrderLine line : items) {
            Optional<StockEntity> stock = stockRepository.findById(line.productId());
            if (stock.isEmpty() || !stock.get().canReserve(line.quantity())) {
                problems.add(line.productId());
            }
        }
        return problems.isEmpty() ? Optional.empty() : Optional.of(problems.getFirst());
    }

    private boolean alreadyProcessed(String eventId) {
        return processedEventRepository.existsById(eventId);
    }

    private void markProcessed(String eventId, String eventType, Instant now) {
        processedEventRepository.save(new ProcessedEventEntity(eventId, eventType, now));
    }

    private void publish(String topic, fr.orderflow.common.event.OrderFlowEvent event, String correlationId) {
        eventPublisher.publish(eventSerializer.envelope(topic, event, correlationId));
    }

    private static String newEventId() {
        return UUID.randomUUID().toString();
    }
}
