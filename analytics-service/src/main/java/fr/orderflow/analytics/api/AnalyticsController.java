package fr.orderflow.analytics.api;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

/**
 * Tableau de bord temps reel (EF-08) : lecture des agregats calcules par Kafka Streams.
 */
@RestController
@RequestMapping("/api/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsQueries queries;

    /**
     * Exemple : {@code {"CREATED":1,"CONFIRMED":12,"CANCELLED":3}}.
     */
    @GetMapping("/orders-by-status")
    public Map<String, Long> ordersByStatus() {
        return queries.ordersByStatus();
    }

    /**
     * Chiffre d'affaires cumule des paiements acceptes. Exemple : {@code {"total":477.60}}.
     */
    @GetMapping("/revenue")
    public Map<String, BigDecimal> revenue() {
        return Map.of("total", queries.revenue());
    }

    /**
     * 503 tant que les state stores ne sont pas prets : le service vient de demarrer ou rejoue ses topics.
     */
    @ExceptionHandler(AnalyticsQueries.StoreNotReadyException.class)
    public ResponseEntity<Map<String, String>> notReady(AnalyticsQueries.StoreNotReadyException e) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(Map.of("message", e.getMessage()));
    }
}
