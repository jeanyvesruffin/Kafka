package fr.orderflow.common.messaging;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CorrelationIdTest {

    @Test
    @DisplayName("Un identifiant raisonnable est conserve tel quel")
    void safeValue_isKept() {
        assertThat(CorrelationId.orGenerate("corr-demo-nominal")).isEqualTo("corr-demo-nominal");
        assertThat(CorrelationId.orGenerate("8f3e.12:ab_C-9")).isEqualTo("8f3e.12:ab_C-9");
    }

    @Test
    @DisplayName("Absent ou vide : un identifiant est genere")
    void missingValue_isGenerated() {
        assertThat(CorrelationId.orGenerate(null)).startsWith("corr-");
        assertThat(CorrelationId.orGenerate("")).startsWith("corr-");
        assertThat(CorrelationId.orGenerate(null)).isNotEqualTo(CorrelationId.orGenerate(null));
    }

    @Test
    @DisplayName("Retour a la ligne, espaces ou longueur excessive : refuse (un client ne peut pas forger de ligne de log)")
    void suspiciousValue_isReplaced() {
        assertThat(CorrelationId.isSafe("abc\nINFO faux log")).isFalse();
        assertThat(CorrelationId.isSafe("abc\r\n")).isFalse();
        assertThat(CorrelationId.isSafe("a b")).isFalse();
        assertThat(CorrelationId.isSafe("x".repeat(65))).isFalse();     // colonne outbox de 64 caracteres
        assertThat(CorrelationId.isSafe("x".repeat(64))).isTrue();
        assertThat(CorrelationId.orGenerate("abc\nINFO faux log")).startsWith("corr-");
    }
}
