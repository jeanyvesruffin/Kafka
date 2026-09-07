package fr.orderflow.common.messaging;

import java.util.Map;

/**
 * Message pret a partir sur le bus : payload deja serialise + metadonnees.
 *
 * <p>La forme est volontairement calquee sur un {@code ProducerRecord} Kafka
 * (topic, cle, valeur, en-tetes). Le jour ou tu implementeras le publisher
 * Kafka, la conversion sera mecanique et sans perte.
 *
 * @param topic   topic de destination (cf. {@link Topics})
 * @param key     cle de partitionnement — toujours l'orderId ici, pour garantir
 *                l'ordre des evenements d'une meme commande
 * @param payload corps du message deja serialise en JSON
 * @param headers en-tetes (cf. {@link EventHeaders})
 */
public record EventEnvelope(
        String topic,
        String key,
        String payload,
        Map<String, String> headers) {
}
