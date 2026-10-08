package fr.orderflow.analytics;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Service Analytics (phase 8, bonus) : agregats temps reel avec Kafka Streams.
 *
 * <p>{@code scanBasePackages} remonte a {@code fr.orderflow} pour recuperer aussi les beans du module
 * commun, comme les autres services. Pas de base de donnees : l'etat vit dans les state stores
 * (RocksDB) de Kafka Streams, reconstructibles a tout moment depuis les topics.
 */
@SpringBootApplication(scanBasePackages = "fr.orderflow")
public class AnalyticsApplication {

    public static void main(String[] args) {
        SpringApplication.run(AnalyticsApplication.class, args);
    }
}
