/**
 * <h2>PACKAGE A REMPLIR — couche messaging du service Commande</h2>
 *
 * Ce package est volontairement vide. C'est ici que tu ecriras la partie Kafka.
 *
 * <h3>1. Le publisher (remplace le publisher de log)</h3>
 * <pre>{@code
 * @Component
 * public class KafkaEventPublisher implements EventPublisher {
 *
 *     private final KafkaTemplate<String, String> kafkaTemplate;
 *
 *     @Override
 *     public void publish(EventEnvelope envelope) {
 *         var record = new ProducerRecord<>(envelope.topic(), envelope.key(), envelope.payload());
 *         envelope.headers().forEach((k, v) ->
 *                 record.headers().add(k, v.getBytes(StandardCharsets.UTF_8)));
 *         kafkaTemplate.send(record);   // .get() si tu veux du synchrone dans le relais d'outbox
 *     }
 * }
 * }</pre>
 *
 * Puis, dans l'{@code application.yml} du service, bascule le publisher :
 * {@code orderflow.messaging.publisher: kafka}. Le publisher de log n'est
 * alors plus cree, et le relais d'outbox se met a publier pour de vrai.
 * Aucune autre ligne du projet n'est a modifier.
 *
 * <h3>2. Les listeners</h3>
 * Le service Commande consomme quatre evenements. Toute la logique metier
 * existe deja dans {@code OrderService} : le listener ne fait que deserialiser
 * et deleguer.
 *
 * <table border="1">
 *   <caption>Topics consommes par order-service</caption>
 *   <tr><th>Topic</th><th>Methode a appeler</th></tr>
 *   <tr><td>inventory.reserved</td><td>{@code orderService.onInventoryReserved(orderId, correlationId)}</td></tr>
 *   <tr><td>inventory.rejected</td><td>{@code orderService.onInventoryRejected(orderId, reason, correlationId)}</td></tr>
 *   <tr><td>payments.completed</td><td>{@code orderService.onPaymentCompleted(orderId, correlationId)}</td></tr>
 *   <tr><td>payments.failed</td><td>{@code orderService.onPaymentFailed(orderId, reason, correlationId)}</td></tr>
 * </table>
 *
 * <pre>{@code
 * @Component
 * public class InventoryEventListener {
 *
 *     @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "order-service")
 *     public void onReserved(@Payload String payload,
 *                            @Header(EventHeaders.CORRELATION_ID) String correlationId) {
 *         var event = eventSerializer.fromJson(payload, InventoryReservedEvent.class);
 *         orderService.onInventoryReserved(event.orderId(), correlationId);
 *     }
 * }
 * }</pre>
 *
 * <h3>Points de vigilance</h3>
 * <ul>
 *   <li>Utilise {@code ErrorHandlingDeserializer} : une erreur de
 *       deserialisation n'est JAMAIS retryable, elle doit partir en DLT
 *       immediatement, sinon le consumer boucle a l'infini sur le message.</li>
 *   <li>Ne mets aucune regle metier ici. Si tu ressens le besoin d'un
 *       {@code if} metier dans un listener, c'est que la methode manque dans
 *       {@code OrderService}.</li>
 *   <li>Propage le {@code correlationId} de l'en-tete jusqu'au MDC des logs.</li>
 * </ul>
 */
package fr.orderflow.order.messaging;
