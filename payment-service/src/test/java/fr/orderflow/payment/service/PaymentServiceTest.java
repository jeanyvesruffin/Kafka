package fr.orderflow.payment.service;

import fr.orderflow.common.event.InventoryReservedEvent;
import fr.orderflow.common.event.OrderLine;
import fr.orderflow.common.messaging.EventEnvelope;
import fr.orderflow.common.messaging.EventPublisher;
import fr.orderflow.common.messaging.EventSerializer;
import fr.orderflow.common.messaging.Topics;
import fr.orderflow.payment.domain.PaymentEntity;
import fr.orderflow.payment.domain.PaymentStatus;
import fr.orderflow.payment.domain.ProcessedEventEntity;
import fr.orderflow.payment.repository.PaymentRepository;
import fr.orderflow.payment.repository.ProcessedEventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import tools.jackson.databind.json.JsonMapper;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentServiceTest {

    private static final Instant NOW = Instant.parse("2026-09-07T10:15:30Z");
    private static final String CID = "corr-test";

    private PaymentRepository paymentRepository;
    private List<EventEnvelope> published;
    private Set<String> processedIds;
    private PaymentService paymentService;

    private static InventoryReservedEvent reserved(String eventId, String amount) {
        var total = new BigDecimal(amount);
        return new InventoryReservedEvent(
                eventId, "ord-1", "cust-118",
                List.of(new OrderLine("sku-001", 1, total)), total, NOW);
    }

    @BeforeEach
    void setUp() {
        published = new ArrayList<>();
        processedIds = new HashSet<>();

        paymentRepository = Mockito.mock(PaymentRepository.class);
        Mockito.when(paymentRepository.save(Mockito.any()))
                .thenAnswer(inv -> inv.getArgument(0));

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
        paymentService = new PaymentService(
                paymentRepository,
                processedRepository,
                new PaymentGatewaySimulator(new BigDecimal("1000.00")),
                publisher,
                new EventSerializer(JsonMapper.builder()
                        .build()),
                Clock.fixed(NOW, ZoneOffset.UTC));
    }

    @Test
    @DisplayName("Montant sous le plafond : paiement accepte et PaymentCompleted publie")
    void underThreshold_completes() {
        paymentService.handleInventoryReserved(reserved("evt-1", "39.80"), CID);

        assertThat(published).hasSize(1);
        assertThat(published.getFirst()
                .topic()).isEqualTo(Topics.PAYMENTS_COMPLETED);
        assertThat(capturePayment().getStatus()).isEqualTo(PaymentStatus.COMPLETED);
    }

    @Test
    @DisplayName("Montant au-dessus du plafond : paiement refuse et PaymentFailed publie")
    void aboveThreshold_fails() {
        paymentService.handleInventoryReserved(reserved("evt-1", "1250.00"), CID);

        assertThat(published).hasSize(1);
        assertThat(published.getFirst()
                .topic()).isEqualTo(Topics.PAYMENTS_FAILED);
        PaymentEntity payment = capturePayment();
        assertThat(payment.getStatus()).isEqualTo(PaymentStatus.FAILED);
        assertThat(payment.getFailureReason()).contains("Plafond depasse");
    }

    // ------------------------------------------------------------------

    @Test
    @DisplayName("Le meme evenement redelivre ne debite pas deux fois (idempotence)")
    void redelivery_doesNotChargeTwice() {
        var event = reserved("evt-1", "39.80");

        paymentService.handleInventoryReserved(event, CID);
        paymentService.handleInventoryReserved(event, CID);

        assertThat(published).hasSize(1);
        Mockito.verify(paymentRepository, Mockito.times(1))
                .save(Mockito.any());
    }

    private PaymentEntity capturePayment() {
        var captor = ArgumentCaptor.forClass(PaymentEntity.class);
        Mockito.verify(paymentRepository, Mockito.atLeastOnce())
                .save(captor.capture());
        return captor.getValue();
    }
}
