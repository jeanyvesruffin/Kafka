package fr.orderflow.common.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import fr.orderflow.common.messaging.Topics;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import tools.jackson.core.JacksonException;

import java.util.List;

/**
 * Fabrique du {@link DefaultErrorHandler} des services : reprises bloquantes a backoff
 * exponentiel, puis Dead Letter Topic.
 *
 * <p><b>Pourquoi ne pas se contenter du gestionnaire par defaut de Boot ?</b> Sans bean
 * {@code CommonErrorHandler}, Spring Kafka re-essaie un message 9 fois a la suite, sans attendre,
 * puis le <b>jette</b> en ne laissant qu'une ligne de log. Un message empoisonne (JSON illisible)
 * serait donc perdu en quelques millisecondes : c'est une violation de NF-01 (aucun message metier
 * perdu). Ici, le message epuise est publie sur {@code <topic>.DLT}, avec l'exception et
 * l'origine (topic, partition, offset, groupe) dans les en-tetes.
 *
 * <p><b>Classification</b> : une erreur qui ne guerit pas en reessayant va <i>directement</i> en
 * DLT, sans reprise.
 * <ul>
 *   <li>non retryable : {@code DeserializationException} (par defaut dans Spring Kafka),
 *       {@link JacksonException} (payload JSON illisible), {@link IllegalArgumentException}
 *       (evenement inattendu), plus ce que le service ajoute ;</li>
 *   <li>retryable : tout le reste, dont {@code OptimisticLockingFailureException} (verrou
 *       optimiste sur le stock) qui disparait des que la transaction relit la valeur a jour.</li>
 * </ul>
 */
@Slf4j
public final class KafkaErrorHandling {

    static final String DLT_METRIC = "orderflow.kafka.dlt";

    private static final List<Class<? extends Exception>> NON_RETRYABLE = List.of(
            JacksonException.class,
            IllegalArgumentException.class);

    private KafkaErrorHandling() {
    }

    /**
     * @param template     template qui publie sur le DLT (doit savoir serialiser {@code String}
     *                     <b>et</b> {@code byte[]} : voir {@link KafkaSupportConfig})
     * @param retry        reglages des reprises bloquantes
     * @param registry     compte les messages envoyes en DLT ({@value #DLT_METRIC}); peut etre null
     * @param nonRetryable exceptions propres au service qui vont directement en DLT
     */
    @SafeVarargs
    public static DefaultErrorHandler blockingRetry(
            KafkaOperations<?, ?> template,
            KafkaRetryProperties.Blocking retry,
            MeterRegistry registry,
            Class<? extends Exception>... nonRetryable) {

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(retry.maxRetries());
        backOff.setInitialInterval(retry.initialInterval().toMillis());
        backOff.setMultiplier(retry.multiplier());
        backOff.setMaxInterval(retry.maxInterval().toMillis());

        DefaultErrorHandler handler = new DefaultErrorHandler(deadLetterRecoverer(template, registry), backOff);
        handler.addNotRetryableExceptions(NON_RETRYABLE.toArray(new Class[0]));
        if (nonRetryable.length > 0) {
            handler.addNotRetryableExceptions(nonRetryable);
        }
        // AckMode.RECORD : on valide l'offset du message envoye en DLT tout de suite. Sinon, un
        // crash juste apres l'envoi le republierait en DLT au redemarrage.
        handler.setCommitRecovered(true);
        return handler;
    }

    /**
     * Publie sur {@code <topic>.DLT} (meme partition), puis compte et journalise.
     *
     * <p>Le nom du DLT est <b>explicite</b> : depuis Spring Kafka 4.0, le {@code DeadLetterPublishingRecoverer}
     * publie par defaut sur {@code <topic>-dlt}. Sans resolveur, le message partirait sur un topic cree a la
     * volee par le broker ({@code orders.created-dlt}) et le {@code orders.created.DLT} declare par le
     * service resterait vide.
     */
    public static ConsumerRecordRecoverer deadLetterRecoverer(KafkaOperations<?, ?> template, MeterRegistry registry) {
        DeadLetterPublishingRecoverer publisher = new DeadLetterPublishingRecoverer(
                template,
                (record, exception) -> new TopicPartition(Topics.dltOf(record.topic()), record.partition()));
        return (ConsumerRecord<?, ?> record, Exception exception) -> {
            Throwable cause = rootCause(exception);
            log.error(
                    "Message envoye en DLT topic={} partition={} offset={} key={} cause={}: {}",
                    record.topic(), record.partition(), record.offset(), record.key(),
                    cause.getClass().getSimpleName(), cause.getMessage());
            countDeadLetter(registry, record.topic(), cause.getClass().getSimpleName());
            publisher.accept(record, exception);
        };
    }

    /**
     * Compteur {@value #DLT_METRIC}{topic, exception} : a surveiller, une valeur non nulle veut dire
     * qu'un message attend une intervention humaine.
     *
     * @param exception nom court de l'exception d'origine (ex. {@code StreamReadException})
     */
    public static void countDeadLetter(MeterRegistry registry, String sourceTopic, String exception) {
        if (registry == null) {
            return;
        }
        Counter.builder(DLT_METRIC)
                .description("Messages envoyes en Dead Letter Topic")
                .tag("topic", sourceTopic)
                .tag("exception", exception)
                .register(registry)
                .increment();
    }

    static Throwable rootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }
}
