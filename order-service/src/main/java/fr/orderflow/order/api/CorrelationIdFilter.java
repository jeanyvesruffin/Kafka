package fr.orderflow.order.api;

import fr.orderflow.common.messaging.CorrelationId;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/**
 * Point d'entree du {@code correlationId} (phase 5, NF-04).
 *
 * <p>Reprend l'en-tete {@code X-Correlation-Id} s'il est present et acceptable, sinon en genere un.
 * Il est :
 * <ul>
 *   <li>place dans le MDC pour toute la duree de la requete (les logs du controller, du service et de
 *       la transaction le portent) ;</li>
 *   <li>expose au controller par un attribut de requete (le controller n'a pas a lire le MDC) ;</li>
 *   <li>renvoye au client dans l'en-tete de la reponse, y compris pour les erreurs.</li>
 * </ul>
 * C'est ensuite l'outbox qui le conserve et le relais qui le met dans l'en-tete Kafka.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class CorrelationIdFilter extends OncePerRequestFilter {

    public static final String REQUEST_ATTRIBUTE = "orderflow.correlationId";

    @Override
    protected void doFilterInternal(
            HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String correlationId = CorrelationId.orGenerate(request.getHeader(CorrelationId.HTTP_HEADER));
        request.setAttribute(REQUEST_ATTRIBUTE, correlationId);
        response.setHeader(CorrelationId.HTTP_HEADER, correlationId);
        MDC.put(CorrelationId.MDC_KEY, correlationId);
        try {
            chain.doFilter(request, response);
        } finally {
            MDC.remove(CorrelationId.MDC_KEY);
        }
    }
}
