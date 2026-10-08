package fr.orderflow.payment.service;

/**
 * Le prestataire de paiement ne repond pas (timeout, 503...) : erreur <b>transitoire</b>.
 *
 * <p>Elle traverse {@code PaymentService.handleInventoryReserved}, ce qui annule sa transaction
 * (le {@code processed_events} inscrit avant l'appel est annule aussi : le rejeu n'est donc pas pris
 * pour un doublon) et declenche les reprises par topics de retry.
 */
public class PaymentGatewayUnavailableException extends RuntimeException {

    public PaymentGatewayUnavailableException(String message) {
        super(message);
    }
}
