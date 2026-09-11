package fr.orderflow.payment.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Simulateur d'encaissement — DETERMINISTE, et c'est volontaire.
 *
 * <p>Un simulateur aleatoire rendrait les tests non reproductibles et le
 * debogage penible. La regle est donc simple et previsible : au-dela d'un
 * plafond configurable, le paiement est refuse.
 *
 * <p>Concretement, avec le catalogue fourni : commander 1x sku-005 (1250,00)
 * declenche un refus et donc tout le parcours de compensation. C'est ton
 * scenario de test du chemin d'echec.
 */
@Component
public class PaymentGatewaySimulator {

    private final BigDecimal refusalThreshold;

    public PaymentGatewaySimulator(
            @Value("${orderflow.payment.refusal-threshold:1000.00}") BigDecimal refusalThreshold) {
        this.refusalThreshold = refusalThreshold;
    }

    public Result charge(String orderId, BigDecimal amount) {
        if (amount.compareTo(refusalThreshold) > 0) {
            return Result.refused("Plafond depasse : " + amount + " > " + refusalThreshold);
        }
        return Result.accepted("txn-" + Integer.toHexString(orderId.hashCode()));
    }

    public record Result(boolean accepted, String transactionId, String reason) {

        static Result accepted(String transactionId) {
            return new Result(true, transactionId, null);
        }

        static Result refused(String reason) {
            return new Result(false, null, reason);
        }
    }
}
