package fr.orderflow.common.messaging;

import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Identifiant de correlation : le fil qui relie, dans les logs de tous les services, tout ce qui se
 * rapporte a une meme commande (NF-04).
 *
 * <p>Il entre par l'en-tete HTTP {@code X-Correlation-Id} (ou est genere), voyage dans l'en-tete
 * Kafka {@code correlationId} de chaque message, et est place dans le MDC des logs a chaque etape
 * (requete HTTP, relais d'outbox, listener Kafka) sous la cle {@link #MDC_KEY}.
 */
public final class CorrelationId {

    /**
     * Cle du MDC, reprise par {@code logging.pattern.correlation} dans les {@code application.yml}.
     */
    public static final String MDC_KEY = "correlationId";

    /**
     * En-tete HTTP d'entree et de sortie de l'API de commande.
     */
    public static final String HTTP_HEADER = "X-Correlation-Id";

    /**
     * Un identifiant fourni de l'exterieur finit dans les logs : on n'accepte que des caracteres
     * inoffensifs, pour qu'un client ne puisse pas y glisser un retour a la ligne et forger de fausses
     * lignes de log. 64 caracteres au plus : c'est la taille de la colonne {@code correlation_id}
     * de l'outbox.
     */
    private static final Pattern SAFE = Pattern.compile("[A-Za-z0-9._:-]{1,64}");

    private CorrelationId() {
    }

    public static String generate() {
        return "corr-" + UUID.randomUUID();
    }

    public static boolean isSafe(String value) {
        return value != null && SAFE.matcher(value).matches();
    }

    /**
     * La valeur recue si elle est acceptable, sinon un nouvel identifiant.
     */
    public static String orGenerate(String received) {
        return isSafe(received) ? received : generate();
    }
}
