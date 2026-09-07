/**
 * <h2>PACKAGE A REMPLIR — couche messaging du service Paiement</h2>
 *
 * <table border="1">
 *   <caption>Topic consomme par payment-service</caption>
 *   <tr><th>Topic</th><th>Methode a appeler</th></tr>
 *   <tr><td>inventory.reserved</td><td>{@code paymentService.handleInventoryReserved(event, correlationId)}</td></tr>
 * </table>
 *
 * <pre>{@code
 * @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "payment-service")
 * public void onInventoryReserved(@Payload String payload,
 *                                 @Header(EventHeaders.CORRELATION_ID) String correlationId) {
 *     paymentService.handleInventoryReserved(
 *             eventSerializer.fromJson(payload, InventoryReservedEvent.class), correlationId);
 * }
 * }</pre>
 *
 * <h3>Bon terrain d'exercice pour la phase 4</h3>
 * Un vrai encaissement appelle un tiers, donc echoue de facon transitoire
 * (timeout reseau, 503). C'est le meilleur endroit du projet pour mettre en
 * place {@code @RetryableTopic} avec backoff exponentiel : ajoute une panne
 * simulee dans {@code PaymentGatewaySimulator} (par exemple echouer les deux
 * premiers appels d'un orderId donne) et observe le message transiter par
 * {@code inventory.reserved-retry-0}, {@code -retry-1}, puis
 * {@code inventory.reserved.DLT}.
 */
package fr.orderflow.payment.messaging;
