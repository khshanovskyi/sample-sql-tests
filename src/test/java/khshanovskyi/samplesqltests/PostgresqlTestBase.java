package khshanovskyi.samplesqltests;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class PostgresqlTestBase {

    protected static EmbeddedPostgres postgres;
    protected static Connection connection;

    @BeforeAll
    static void startDatabase() throws IOException {
        postgres = EmbeddedPostgres.start();
        try {
            connection = postgres.getPostgresDatabase().getConnection();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get database connection", e);
        }
    }

    @AfterAll
    static void stopDatabase() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                // Log error
            }
        }

        if (postgres != null) {
            postgres.close();
        }
    }

    @BeforeEach
    void setupSchema() throws SQLException, IOException {
        // Optional: Clear database before each test
        // clearDatabase();

        // Initialize database schema and test data
        executeSqlFile("src/test/resources/schema.sql");
        executeSqlFile("src/test/resources/test-data.sql");
    }

    private void clearDatabase() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            // Drop all tables, views, etc.
            statement.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;");
        }
    }

    protected void executeSqlFile(String filePath) throws IOException, SQLException {
        Path path = Paths.get(filePath);
        String sql = Files.readString(path);

        try (Statement statement = connection.createStatement()) {
            // Split by semicolon to execute multiple statements
            for (String query : sql.split(";")) {
                if (!query.trim().isEmpty()) {
                    statement.execute(query);
                }
            }
        }
    }
}
