package fr.orderflow.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Service Commande.
 *
 * <p>{@code scanBasePackages} remonte a {@code fr.orderflow} pour recuperer
 * aussi les beans du module commun (EventSerializer, publisher par defaut).
 *
 * <p>{@code @EnableScheduling} sert au relais d'outbox.
 */
@SpringBootApplication(scanBasePackages = "fr.orderflow")
@EnableScheduling
public class OrderApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}
