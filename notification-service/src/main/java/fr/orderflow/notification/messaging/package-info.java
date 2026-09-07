/**
 * <h2>PACKAGE A REMPLIR — couche messaging du service Notification</h2>
 *
 * Ce service consomme les evenements terminaux :
 * {@code orders.confirmed} et {@code orders.cancelled}.
 *
 * <pre>{@code
 * @KafkaListener(topics = { Topics.ORDERS_CONFIRMED, Topics.ORDERS_CANCELLED },
 *                groupId = "notification-service")
 * public void onTerminalEvent(@Payload String payload,
 *                             @Header(EventHeaders.EVENT_TYPE) String eventType,
 *                             @Header(EventHeaders.CORRELATION_ID) String correlationId) {
 *     OrderFlowEvent event = switch (eventType) {
 *         case OrderConfirmedEvent.TYPE -> eventSerializer.fromJson(payload, OrderConfirmedEvent.class);
 *         case OrderCancelledEvent.TYPE -> eventSerializer.fromJson(payload, OrderCancelledEvent.class);
 *         default -> throw new IllegalArgumentException("Type inattendu : " + eventType);
 *     };
 *     notificationService.notifyCustomer(event, correlationId);
 * }
 * }</pre>
 *
 * <h3>La question a se poser ici</h3>
 * Ce service n'a pas de table {@code processed_events} : il n'est donc PAS
 * idempotent. En at-least-once, le client peut recevoir deux fois le meme
 * e-mail. Est-ce acceptable ? Pour une notification, souvent oui. Pour un
 * debit bancaire, jamais. Savoir ou l'idempotence est indispensable et ou elle
 * est du luxe fait partie de l'exercice.
 */
package fr.orderflow.notification.messaging;
