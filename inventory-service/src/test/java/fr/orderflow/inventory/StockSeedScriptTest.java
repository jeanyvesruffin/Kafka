package fr.orderflow.inventory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ScriptUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Le script {@code data.sql} rejoue a chaque demarrage du service (base H2 fichier, donnees conservees
 * entre deux demarrages). Il ne doit jamais ecraser un stock existant.
 *
 * <p>Regression trouvee par le test de chaos de la phase 9 : le script d'origine ({@code MERGE ... KEY})
 * remettait chaque produit a son stock initial a chaque redemarrage d'inventory-service.
 */
class StockSeedScriptTest {

    private static Connection database() throws SQLException {
        Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:seed-" + UUID.randomUUID() + ";MODE=PostgreSQL;DB_CLOSE_DELAY=-1", "sa", "");
        try (Statement statement = connection.createStatement()) {
            // Meme schema que StockEntity (ddl-auto: update le cree dans le vrai service)
            statement.execute("CREATE TABLE stock (product_id VARCHAR(64) PRIMARY KEY, "
                    + "quantity_available INT NOT NULL, quantity_reserved INT NOT NULL, version BIGINT)");
        }
        return connection;
    }

    private static void seed(Connection connection) {
        ScriptUtils.executeSqlScript(connection, new ClassPathResource("data.sql"));
    }

    private static int[] stock(Connection connection, String productId) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery(
                     "SELECT quantity_available, quantity_reserved, version FROM stock WHERE product_id = '" + productId + "'")) {
            assertThat(rs.next()).as("produit %s present", productId).isTrue();
            return new int[] {rs.getInt(1), rs.getInt(2), rs.getInt(3)};
        }
    }

    @Test
    @DisplayName("Base vide : les cinq produits du catalogue sont inseres")
    void emptyDatabase_isSeeded() throws SQLException {
        try (Connection connection = database()) {
            seed(connection);

            assertThat(stock(connection, "sku-001")).containsExactly(100, 0, 0);
            assertThat(stock(connection, "sku-003")).containsExactly(500, 0, 0);
            assertThat(stock(connection, "sku-005")).containsExactly(2, 0, 0);
        }
    }

    @Test
    @DisplayName("Redemarrage : un stock deja entame (reservations, version) n'est PAS remis a sa valeur initiale")
    void restart_doesNotResetExistingStock() throws SQLException {
        try (Connection connection = database()) {
            seed(connection);
            try (Statement statement = connection.createStatement()) {
                // 3 unites reservees et 7 passages : l'etat d'un service qui a deja travaille
                statement.execute("UPDATE stock SET quantity_available = 97, quantity_reserved = 3, version = 7 "
                        + "WHERE product_id = 'sku-001'");
            }

            seed(connection);    // le service redemarre : le script est rejoue

            assertThat(stock(connection, "sku-001")).containsExactly(97, 3, 7);
        }
    }

    @Test
    @DisplayName("Un produit ajoute au catalogue apres coup est insere sans toucher aux autres")
    void missingProduct_isInsertedWithoutTouchingTheOthers() throws SQLException {
        try (Connection connection = database()) {
            seed(connection);
            try (Statement statement = connection.createStatement()) {
                statement.execute("UPDATE stock SET quantity_available = 40, quantity_reserved = 10 WHERE product_id = 'sku-002'");
                statement.execute("DELETE FROM stock WHERE product_id = 'sku-004'");
            }

            seed(connection);

            assertThat(stock(connection, "sku-002")).containsExactly(40, 10, 0);
            assertThat(stock(connection, "sku-004")).containsExactly(10, 0, 0);
        }
    }
}
