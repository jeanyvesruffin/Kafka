package fr.orderflow.payment.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
 *
 * <h3>Panne transitoire (phase 4)</h3>
 * {@code orderflow.payment.transient-failures-per-order=N} (0 par defaut) fait echouer les
 * <b>N premiers appels</b> de chaque commande avec une {@link PaymentGatewayUnavailableException}, puis
 * laisse passer les suivants. Toujours deterministe : avec 2, le message transite par
 * {@code inventory.reserved-retry-0} et {@code -retry-1} avant d'aboutir ; avec une valeur superieure
 * ou egale au nombre de tentatives (3 par defaut), il finit sur {@code inventory.reserved.DLT}.
 *
 * <p>Le decompte est tenu en memoire, par commande : il repart de zero au redemarrage du service, ce
 * qui suffit pour observer les reprises. Un rejeu manuel depuis le DLT, une fois le seuil depasse,
 * reussit donc, comme le ferait un prestataire revenu en ligne.
 */
@Component
public class PaymentGatewaySimulator {

    private final BigDecimal refusalThreshold;
    private final int transientFailuresPerOrder;
    private final Map<String, Integer> callsByOrder = new ConcurrentHashMap<>();

    public PaymentGatewaySimulator(BigDecimal refusalThreshold) {
        this(refusalThreshold, 0);
    }

    @Autowired
    public PaymentGatewaySimulator(
            @Value("${orderflow.payment.refusal-threshold:1000.00}") BigDecimal refusalThreshold,
            @Value("${orderflow.payment.transient-failures-per-order:0}") int transientFailuresPerOrder) {
        this.refusalThreshold = refusalThreshold;
        this.transientFailuresPerOrder = transientFailuresPerOrder;
    }

    public Result charge(String orderId, BigDecimal amount) {
        if (transientFailuresPerOrder > 0) {
            int call = callsByOrder.merge(orderId, 1, Integer::sum);
            if (call <= transientFailuresPerOrder) {
                throw new PaymentGatewayUnavailableException(
                        "Prestataire de paiement indisponible (simulation) orderId=" + orderId
                                + " appel " + call + "/" + transientFailuresPerOrder);
            }
        }
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
