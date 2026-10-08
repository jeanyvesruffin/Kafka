package fr.orderflow.common.kafka;

import fr.orderflow.common.messaging.CorrelationId;
import fr.orderflow.common.messaging.EventHeaders;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

import java.nio.charset.StandardCharsets;

/**
 * Propage le {@code correlationId} de l'en-tete Kafka vers le MDC des logs (phase 5).
 *
 * <p>Boot applique tout seul le {@code RecordInterceptor} unique du contexte a la fabrique de
 * conteneurs, donc a tous les {@code @KafkaListener} du service, y compris ceux des topics de retry.
 * Le MDC est pose avant l'appel du listener et retire apres la fin du traitement, gestion d'erreur
 * comprise : les logs du listener, du service metier, du publisher et du {@code DefaultErrorHandler}
 * portent donc tous l'identifiant du message en cours.
 *
 * <p>Un en-tete absent ou suspect (voir {@link CorrelationId#isSafe}) laisse le MDC vide : le log
 * montre alors {@code [-]} plutot qu'une valeur inventee qui laisserait croire a une correlation.
 */
public class CorrelationIdRecordInterceptor<K, V> implements RecordInterceptor<K, V> {

    @Override
    public ConsumerRecord<K, V> intercept(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        Header header = record.headers().lastHeader(EventHeaders.CORRELATION_ID);
        if (header != null && header.value() != null) {
            String correlationId = new String(header.value(), StandardCharsets.UTF_8);
            if (CorrelationId.isSafe(correlationId)) {
                MDC.put(CorrelationId.MDC_KEY, correlationId);
            }
        }
        return record;
    }

    @Override
    public void afterRecord(ConsumerRecord<K, V> record, Consumer<K, V> consumer) {
        MDC.remove(CorrelationId.MDC_KEY);
    }
}
