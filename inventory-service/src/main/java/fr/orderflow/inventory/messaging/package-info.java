/**
 * <h2>PACKAGE A REMPLIR — couche messaging du service Stock</h2>
 * <p>
 * Toute la logique metier existe deja dans {@code InventoryService}, deja
 * idempotente. Ton listener deserialise et delegue, rien de plus.
 *
 * <table border="1">
 *   <caption>Topics consommes par inventory-service</caption>
 *   <tr><th>Topic</th><th>Methode a appeler</th></tr>
 *   <tr><td>orders.created</td><td>{@code inventoryService.handleOrderCreated(event, correlationId)}</td></tr>
 *   <tr><td>orders.cancelled</td><td>{@code inventoryService.handleOrderCancelled(event)} (compensation)</td></tr>
 * </table>
 *
 * <pre>{@code
 * @Component
 * public class OrderEventListener {
 *
 *     @KafkaListener(topics = Topics.ORDERS_CREATED, groupId = "inventory-service")
 *     public void onOrderCreated(@Payload String payload,
 *                                @Header(EventHeaders.CORRELATION_ID) String correlationId) {
 *         inventoryService.handleOrderCreated(
 *                 eventSerializer.fromJson(payload, OrderCreatedEvent.class), correlationId);
 *     }
 * }
 * }</pre>
 *
 * <h3>Le piege a ne pas rater ici</h3>
 * {@code StockEntity} porte un {@code @Version} (verrouillage optimiste). Deux
 * commandes concurrentes sur le meme produit peuvent donc produire une
 * {@code OptimisticLockingFailureException}. Ton {@code DefaultErrorHandler}
 * doit la classer RETRYABLE — au reessai, la transaction relit le stock a jour
 * et passe. A l'inverse, une {@code DeserializationException} doit aller en DLT
 * sans le moindre reessai.
 *
 * <p>C'est precisement l'exercice de la phase 4 : distinguer erreur transitoire
 * et erreur definitive.
 */
package fr.orderflow.inventory.messaging;
