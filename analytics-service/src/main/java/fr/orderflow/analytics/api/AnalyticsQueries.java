package fr.orderflow.analytics.api;

import fr.orderflow.analytics.topology.AnalyticsTopology;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Interrogation des state stores ("interactive queries") : lire les agregats sans passer par Kafka.
 *
 * <p>Un seul noeud ici, donc toutes les cles sont locales. Avec plusieurs instances, chacune ne
 * detiendrait qu'une partie des cles : il faudrait declarer {@code application.server} et router la
 * requete vers l'instance qui possede la cle.
 */
@Service
@RequiredArgsConstructor
public class AnalyticsQueries {

    private final StreamsBuilderFactoryBean streams;

    /**
     * Commandes par statut. Les trois statuts sont toujours presents, a 0 s'ils n'ont aucune commande.
     */
    public Map<String, Long> ordersByStatus() {
        ReadOnlyKeyValueStore<String, Long> store = store(AnalyticsTopology.ORDERS_BY_STATUS);
        Map<String, Long> result = new LinkedHashMap<>();
        AnalyticsTopology.STATUSES.forEach(status -> result.put(status, 0L));
        try (KeyValueIterator<String, Long> all = store.all()) {
            all.forEachRemaining(entry -> result.put(entry.key, entry.value));
        }
        return result;
    }

    public BigDecimal revenue() {
        ReadOnlyKeyValueStore<String, BigDecimal> store = store(AnalyticsTopology.REVENUE);
        BigDecimal total = store.get(AnalyticsTopology.REVENUE_KEY);
        return total == null ? BigDecimal.ZERO : total;
    }

    private <V> ReadOnlyKeyValueStore<String, V> store(String name) {
        KafkaStreams kafkaStreams = streams.getKafkaStreams();
        if (kafkaStreams == null) {
            throw new StoreNotReadyException("Kafka Streams n'est pas demarre");
        }
        try {
            return kafkaStreams.store(StoreQueryParameters.fromNameAndType(name, QueryableStoreTypes.keyValueStore()));
        } catch (InvalidStateStoreException e) {
            // Demarrage, rebalance ou restauration depuis le changelog en cours
            throw new StoreNotReadyException("State store '" + name + "' pas encore disponible (" + kafkaStreams.state() + ")");
        }
    }

    public static class StoreNotReadyException extends RuntimeException {
        public StoreNotReadyException(String message) {
            super(message);
        }
    }
}
