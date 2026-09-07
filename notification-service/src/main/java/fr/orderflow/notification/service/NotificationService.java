package fr.orderflow.notification.service;

import fr.orderflow.common.event.OrderCancelledEvent;
import fr.orderflow.common.event.OrderConfirmedEvent;
import fr.orderflow.common.event.OrderFlowEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Notification client — sans etat, sans base.
 *
 * <p>Illustre le principal benefice de l'architecture evenementielle : ce
 * service a ete ajoute a la chaine sans qu'aucune ligne des services Commande,
 * Stock ou Paiement n'ait a changer. C'est l'exigence NF-07 rendue concrete.
 *
 * <p>Le {@code switch} sur l'interface scellee est exhaustif : le compilateur
 * refusera de compiler si tu ajoutes un evenement terminal sans traiter son cas.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    public void notifyCustomer(OrderFlowEvent event, String correlationId) {
        String message = switch (event) {
            case OrderConfirmedEvent e ->
                    "Votre commande " + e.orderId() + " est confirmee. Merci !";
            case OrderCancelledEvent e ->
                    "Votre commande " + e.orderId() + " a ete annulee. Motif : " + e.reason();
            default ->
                    null;   // les evenements intermediaires ne declenchent pas de notification
        };

        if (message == null) {
            log.debug("Evenement non notifiable, ignore type={}", event.eventType());
            return;
        }
        log.info("[NOTIFICATION] orderId={} correlationId={} message=\"{}\"",
                event.orderId(), correlationId, message);
    }
}
