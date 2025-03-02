package khshanovskyi.samplesqltests;

import org.junit.jupiter.api.Test;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class SqlQueriesTest {

    private static EmbeddedPostgres postgres;
    private static Connection connection;

    // File paths relative to src/test/resources
    private static final String SCHEMA_FILE = "schema.sql";
    private static final String TEST_DATA_FILE = "test-data.sql";
    private static final String QUERIES_DIR = "queries/";

    @BeforeAll
    static void startDatabase() throws IOException {
        System.out.println("Starting embedded PostgreSQL...");
        postgres = EmbeddedPostgres.start();
        try {
            connection = postgres.getPostgresDatabase().getConnection();
            connection.setAutoCommit(false); // For transaction control
            System.out.println("PostgreSQL started successfully");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to get database connection", e);
        }
    }

    @AfterAll
    static void stopDatabase() throws IOException {
        System.out.println("Stopping embedded PostgreSQL...");
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                System.err.println("Error closing connection: " + e.getMessage());
            }
        }

        if (postgres != null) {
            postgres.close();
            System.out.println("PostgreSQL stopped successfully");
        }
    }

    @BeforeEach
    void setupSchema() throws SQLException, IOException {
        System.out.println("Setting up database schema and test data...");
        // Start a transaction that will be rolled back after each test
        connection.setAutoCommit(false);

        // Clear any existing data
        clearDatabase();

        // Initialize database schema
        executeSqlFile(getResourcePath(SCHEMA_FILE));

        // Load test data
        executeSqlFile(getResourcePath(TEST_DATA_FILE));

        System.out.println("Setup complete");
    }

    private void clearDatabase() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;");
        }
    }

    private void executeSqlFile(String filePath) throws IOException, SQLException {
        Path path = Paths.get(filePath);
        String sql = Files.readString(path);

        try (Statement statement = connection.createStatement()) {
            // Execute each statement separately
            for (String query : sql.split(";")) {
                if (!query.trim().isEmpty()) {
                    statement.execute(query);
                }
            }
        }
    }

    private String getResourcePath(String resourceName) {
        // In a real application, you would use a resource loader
        // Here we're simplifying by using a relative path
        return "src/test/resources/" + resourceName;
    }

    @Test
    void testEmployeesByDepartment() throws IOException, SQLException {
        // Execute the query
        List<Map<String, Object>> results = executeQueryFromFile("employee_by_department.sql");

        // Verify results
        assertFalse(results.isEmpty(), "Query should return results");
        assertEquals(7, results.size(), "Should have 7 active employees");

        // Verify Engineering department employees
        List<Map<String, Object>> engineeringEmployees = results.stream()
                .filter(row -> "Engineering".equals(row.get("department_name")))
                .toList();

        assertEquals(2, engineeringEmployees.size(), "Should have 2 active Engineering employees");

        // Check specific employee
        Map<String, Object> johnSmith = results.stream()
                .filter(row -> "Smith".equals(row.get("last_name")) && "John".equals(row.get("first_name")))
                .findFirst()
                .orElse(null);

        assertNotNull(johnSmith, "John Smith should be in the results");
        assertEquals("Engineering", johnSmith.get("department_name"));
        assertEquals(75000.00, ((Number) johnSmith.get("salary")).doubleValue());
    }

    @Test
    void testProjectSummary() throws IOException, SQLException {
        // Execute the query
        List<Map<String, Object>> results = executeQueryFromFile("project_summary.sql");

        // Verify results
        assertEquals(5, results.size(), "Should have 5 projects");

        // Verify project with most team members
        Map<String, Object> mobileApp = results.stream()
                .filter(row -> "Mobile App Development".equals(row.get("project_name")))
                .findFirst()
                .orElse(null);

        assertNotNull(mobileApp, "Mobile App Development project should exist");
        assertEquals(3, ((Number) mobileApp.get("employee_count")).intValue());
        assertEquals(91666.67, ((Number) mobileApp.get("budget_per_employee")).doubleValue(), 0.01);

        // Verify completed project
        Map<String, Object> annualAudit = results.stream()
                .filter(row -> "Annual Audit".equals(row.get("project_name")))
                .findFirst()
                .orElse(null);

        assertNotNull(annualAudit, "Annual Audit project should exist");
        assertEquals("COMPLETED", annualAudit.get("status"));
    }

    @Test
    void testDepartmentSalaryStats() throws IOException, SQLException {
        // Execute the query
        List<Map<String, Object>> results = executeQueryFromFile("department_salary_stats.sql");

        // Verify results
        assertEquals(4, results.size(), "Should have stats for 4 departments");

        // Verify department with highest average salary
        Map<String, Object> topDepartment = results.get(0); // Results are ordered by avg_salary DESC
        assertEquals("Finance", topDepartment.get("department_name"));

        // Find Engineering department stats
        Map<String, Object> engineering = results.stream()
                .filter(row -> "Engineering".equals(row.get("department_name")))
                .findFirst()
                .orElse(null);

        assertNotNull(engineering, "Engineering department should be in the results");
        assertEquals(2, ((Number) engineering.get("employee_count")).intValue());
        assertEquals(78500.00, ((Number) engineering.get("avg_salary")).doubleValue(), 0.01);
        assertEquals(75000.00, ((Number) engineering.get("min_salary")).doubleValue());
        assertEquals(82000.00, ((Number) engineering.get("max_salary")).doubleValue());
        assertEquals(157000.00, ((Number) engineering.get("total_salary_expense")).doubleValue());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "employee_by_department.sql",
            "project_summary.sql",
            "department_salary_stats.sql"
    })
    void testQuerySyntax(String queryFile) throws IOException {
        // This test just ensures the query can be executed without syntax errors
        assertDoesNotThrow(() -> executeQueryFromFile(queryFile));
    }

    private List<Map<String, Object>> executeQueryFromFile(String queryFileName) throws IOException, SQLException {
        String filePath = getResourcePath(QUERIES_DIR + queryFileName);
        String sql = Files.readString(Paths.get(filePath));

        List<Map<String, Object>> results = new ArrayList<>();

        try (Statement statement = connection.createStatement();
             ResultSet resultSet = statement.executeQuery(sql)) {

            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (resultSet.next()) {
                Map<String, Object> row = new HashMap<>();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    Object value = resultSet.getObject(i);
                    row.put(columnName, value);
                }

                results.add(row);
            }
        }

        return results;
    }
}