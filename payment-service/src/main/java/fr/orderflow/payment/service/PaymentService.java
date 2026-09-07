package fr.orderflow.payment.service;

import fr.orderflow.common.event.InventoryReservedEvent;
import fr.orderflow.common.event.OrderFlowEvent;
import fr.orderflow.common.event.PaymentCompletedEvent;
import fr.orderflow.common.event.PaymentFailedEvent;
import fr.orderflow.common.messaging.EventPublisher;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.payment.domain.PaymentEntity;
import fr.orderflow.payment.domain.PaymentStatus;
import fr.orderflow.payment.domain.ProcessedEventEntity;
import fr.orderflow.payment.repository.PaymentRepository;
import fr.orderflow.payment.repository.ProcessedEventRepository;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Encaissement simule.
 *
 * <p>Point d'entree unique : {@link #handleInventoryReserved}, idempotent,
 * a appeler depuis ton futur listener sur {@code inventory.reserved}.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final PaymentRepository paymentRepository;
    private final ProcessedEventRepository processedEventRepository;
    private final PaymentGatewaySimulator gateway;
    private final EventPublisher eventPublisher;
    private final EventSerializer eventSerializer;
    private final Clock clock;

    public PaymentService(PaymentRepository paymentRepository,
                          ProcessedEventRepository processedEventRepository,
                          PaymentGatewaySimulator gateway,
                          EventPublisher eventPublisher,
                          EventSerializer eventSerializer,
                          Clock clock) {
        this.paymentRepository = paymentRepository;
        this.processedEventRepository = processedEventRepository;
        this.gateway = gateway;
        this.eventPublisher = eventPublisher;
        this.eventSerializer = eventSerializer;
        this.clock = clock;
    }

    @Transactional
    public void handleInventoryReserved(InventoryReservedEvent event, String correlationId) {
        if (processedEventRepository.existsById(event.eventId())) {
            log.debug("Evenement deja traite, ignore eventId={}", event.eventId());
            return;
        }
        Instant now = clock.instant();
        processedEventRepository.save(
                new ProcessedEventEntity(event.eventId(), event.eventType(), now));

        var result = gateway.charge(event.orderId(), event.totalAmount());
        String paymentId = "pay-" + UUID.randomUUID().toString().substring(0, 8);

        if (result.accepted()) {
            paymentRepository.save(new PaymentEntity(paymentId, event.orderId(),
                    PaymentStatus.COMPLETED, event.totalAmount(), null, now));
            log.info("Paiement accepte orderId={} montant={} txn={}",
                    event.orderId(), event.totalAmount(), result.transactionId());
            publish(Topics.PAYMENTS_COMPLETED, new PaymentCompletedEvent(
                    newEventId(), event.orderId(), event.totalAmount(), result.transactionId(), now),
                    correlationId);
        } else {
            paymentRepository.save(new PaymentEntity(paymentId, event.orderId(),
                    PaymentStatus.FAILED, event.totalAmount(), result.reason(), now));
            log.info("Paiement refuse orderId={} montant={} raison={}",
                    event.orderId(), event.totalAmount(), result.reason());
            publish(Topics.PAYMENTS_FAILED, new PaymentFailedEvent(
                    newEventId(), event.orderId(), event.totalAmount(), result.reason(), now),
                    correlationId);
        }
    }

    @Transactional(readOnly = true)
    public List<PaymentEntity> findByOrderId(String orderId) {
        return paymentRepository.findByOrderId(orderId);
    }

    private void publish(String topic, OrderFlowEvent event, String correlationId) {
        eventPublisher.publish(eventSerializer.envelope(topic, event, correlationId));
    }

    private static String newEventId() {
        return UUID.randomUUID().toString();
    }
}
