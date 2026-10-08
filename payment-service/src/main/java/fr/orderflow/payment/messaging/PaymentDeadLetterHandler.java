package fr.orderflow.payment.messaging;

import fr.orderflow.common.kafka.KafkaErrorHandling;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Derniere etape : le message arrive sur {@code inventory.reserved.DLT} apres epuisement des
 * reprises (ou immediatement pour une erreur non retryable).
 *
 * <p>Il ne fait que <b>signaler</b> : journal en ERROR et compteur {@code orderflow.kafka.dlt}. Le
 * message reste dans le DLT (14 jours) pour inspection et rejeu manuel depuis AKHQ. Pas de logique
 * metier ici : decider d'annuler la commande, de relancer le paiement ou de contacter le client est
 * une decision de gestion, pas un reflexe technique.
 *
 * <p>Consequence a connaitre : tant que le message est dans le DLT, la commande reste
 * {@code INVENTORY_RESERVED} avec son stock reserve. C'est le prix d'un DLT sans compensation
 * automatique, et la raison d'etre de l'alerte sur le compteur.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "orderflow.messaging.publisher", havingValue = "kafka")
@RequiredArgsConstructor
public class PaymentDeadLetterHandler {

    /**
     * Groupe du listener de {@code InventoryEventListener}.
     */
    static final String OWN_CONSUMER_GROUP = "payment-service";

    private final ObjectProvider<MeterRegistry> meterRegistry;

    /**
     * Les topics de retry posent leurs en-tetes sous {@code kafka_original-*} / {@code kafka_exception-*}
     * (et non {@code kafka_dlt-*} comme le {@code DeadLetterPublishingRecoverer} d'un
     * {@code DefaultErrorHandler}). L'exception du listener enveloppe la vraie cause : c'est
     * {@code kafka_exception-cause-fqcn} qui nomme l'erreur d'origine.
     */
    public void onDeadLetter(ConsumerRecord<String, String> record) {
        // inventory.reserved.DLT est partage : order-service lit aussi inventory.reserved et y envoie
        // ses propres echecs. Ce handler ne parle que de ceux de payment-service.
        String originalGroup = header(record, KafkaHeaders.ORIGINAL_CONSUMER_GROUP);
        if (originalGroup != null && !OWN_CONSUMER_GROUP.equals(originalGroup)) {
            log.debug("DLT d'un autre service ignore groupe={} orderId={}", originalGroup, record.key());
            return;
        }
        String originalTopic = header(record, KafkaHeaders.ORIGINAL_TOPIC);
        String exception = header(record, KafkaHeaders.EXCEPTION_CAUSE_FQCN);
        if (exception == null) {
            exception = header(record, KafkaHeaders.EXCEPTION_FQCN);
        }
        log.error(
                "PAIEMENT NON TRAITE, message en DLT orderId={} topicOrigine={} offsetOrigine={} exception={} : {}",
                record.key(), originalTopic, header(record, KafkaHeaders.ORIGINAL_OFFSET), exception,
                header(record, KafkaHeaders.EXCEPTION_MESSAGE));
        // kafka_original-topic garde le topic d'origine (inventory.reserved), pas le dernier topic de retry
        KafkaErrorHandling.countDeadLetter(
                meterRegistry.getIfAvailable(), originalTopic != null ? originalTopic : record.topic(),
                exception != null ? exception.substring(exception.lastIndexOf('.') + 1) : "inconnue");
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null || header.value() == null) {
            return null;
        }
        // ORIGINAL_OFFSET est un long encode sur 8 octets, les autres sont du texte UTF-8
        if (KafkaHeaders.ORIGINAL_OFFSET.equals(name) && header.value().length == Long.BYTES) {
            return String.valueOf(ByteBuffer.wrap(header.value()).getLong());
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
