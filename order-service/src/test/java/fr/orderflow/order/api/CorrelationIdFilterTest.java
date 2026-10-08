package fr.orderflow.order.api;

import fr.orderflow.common.messaging.CorrelationId;
import jakarta.servlet.ServletException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests unitaires purs du filtre : aucun contexte Spring, aucun serveur.
 */
class CorrelationIdFilterTest {

    private final CorrelationIdFilter filter = new CorrelationIdFilter();

    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    @Test
    @DisplayName("En-tete fourni : repris dans le MDC pendant la requete, l'attribut et la reponse, puis retire du MDC")
    void providedHeader_isPropagated() throws ServletException, IOException {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationId.HTTP_HEADER, "corr-demo-nominal");
        MockHttpServletResponse response = new MockHttpServletResponse();
        AtomicReference<String> mdcDuringRequest = new AtomicReference<>();

        filter.doFilter(request, response, new MockFilterChain() {
            @Override
            public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                mdcDuringRequest.set(MDC.get(CorrelationId.MDC_KEY));
            }
        });

        assertThat(mdcDuringRequest).hasValue("corr-demo-nominal");
        assertThat(request.getAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE)).isEqualTo("corr-demo-nominal");
        assertThat(response.getHeader(CorrelationId.HTTP_HEADER)).isEqualTo("corr-demo-nominal");
        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }

    @Test
    @DisplayName("Pas d'en-tete : un identifiant est genere et renvoye au client")
    void missingHeader_isGenerated() throws ServletException, IOException {
        MockHttpServletRequest request = new MockHttpServletRequest();
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        String generated = response.getHeader(CorrelationId.HTTP_HEADER);
        assertThat(generated).startsWith("corr-");
        assertThat(request.getAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE)).isEqualTo(generated);
    }

    @Test
    @DisplayName("En-tete avec retour a la ligne (forge de log) : ignore, un identifiant propre est genere")
    void suspiciousHeader_isReplaced() throws ServletException, IOException {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationId.HTTP_HEADER, "abc\n2026-10-08 INFO faux log");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, new MockFilterChain());

        assertThat(response.getHeader(CorrelationId.HTTP_HEADER)).startsWith("corr-").doesNotContain("\n");
    }

    @Test
    @DisplayName("Meme si la requete echoue, le MDC est nettoye (le thread sert une autre requete ensuite)")
    void mdcIsClearedOnFailure() {
        MockHttpServletRequest request = new MockHttpServletRequest();
        request.addHeader(CorrelationId.HTTP_HEADER, "corr-boom");

        try {
            filter.doFilter(request, new MockHttpServletResponse(), new MockFilterChain() {
                @Override
                public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                    throw new IllegalStateException("boom");
                }
            });
        } catch (Exception expected) {
            // attendu
        }

        assertThat(MDC.get(CorrelationId.MDC_KEY)).isNull();
    }
}
