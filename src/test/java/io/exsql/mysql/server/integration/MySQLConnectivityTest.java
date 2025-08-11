package io.exsql.mysql.server.integration;

import io.exsql.mysql.server.util.MySQLClientTestBase;
import io.exsql.mysql.server.util.TestDataGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for basic MySQL client connectivity.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class MySQLConnectivityTest extends MySQLClientTestBase {

    @BeforeAll
    static void setupTestData() {
        // Create some test data for queries
        TestDataGenerator.registerAllTestTables(spark);
    }

    @Test
    @DisplayName("01. Test basic connection")
    void testBasicConnection() throws SQLException {
        assertCanConnect();
    }

    @Test
    @DisplayName("02. Test simple query execution")
    void testSimpleQuery() throws SQLException {
        List<List<Object>> results = executeQuery("SELECT 1 as test_value");
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).containsExactly(1);
    }

    @Test
    @DisplayName("03. Test version comment query")
    void testVersionComment() throws SQLException {
        List<List<Object>> results = executeQuery("SELECT @@version_comment LIMIT 1");
        assertThat(results).hasSize(1);
        assertThat(results.get(0)).containsExactly("data-platform-mysql-frontend");
    }

    @Test
    @DisplayName("04. Test SHOW DATABASES")
    void testShowDatabases() throws SQLException {
        List<List<Object>> results = executeQuery("SHOW DATABASES");
        
        assertThat(results).isNotEmpty();
        
        // Extract database names
        List<String> databaseNames = results.stream()
            .map(row -> (String) row.get(0))
            .toList();
        
        // Should contain at least default and information_schema
        assertThat(databaseNames).contains("default", "information_schema");
    }

    @Test
    @DisplayName("05. Test USE database statement")
    void testUseDatabaseStatement() throws SQLException {
        try (Connection conn = createConnection()) {
            // Switch to default database
            executeUpdate(conn, "USE default");
            
            // Verify we can query tables in this database
            List<List<Object>> results = executeQuery(conn, "SHOW TABLES");
            
            List<String> tableNames = results.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(tableNames).contains("default_persons", "default_orders");
        }
    }

    @Test
    @DisplayName("06. Test information_schema database access")
    void testInformationSchemaAccess() throws SQLException {
        try (Connection conn = createConnection()) {
            // Switch to information_schema database
            executeUpdate(conn, "USE information_schema");
            
            // Query SCHEMATA table
            List<List<Object>> results = executeQuery(conn, "SELECT SCHEMA_NAME FROM SCHEMATA");
            
            assertThat(results).isNotEmpty();
            
            List<String> schemaNames = results.stream()
                .map(row -> (String) row.get(1)) // SCHEMA_NAME is column 2
                .toList();
            
            assertThat(schemaNames).contains("information_schema", "default");
        }
    }

    @Test
    @DisplayName("07. Test query on Spark table")
    void testSparkTableQuery() throws SQLException {
        List<List<Object>> results = executeQuery("SELECT COUNT(*) FROM default.default_persons");
        
        assertThat(results).hasSize(1);
        
        // Should have the number of persons we created (50)
        Object count = results.get(0).get(0);
        assertThat(count).isEqualTo(50L);
    }

    @Test
    @DisplayName("08. Test concurrent connections")
    void testConcurrentConnections() throws SQLException {
        // Create multiple connections and verify they all work
        Connection conn1 = createConnection();
        Connection conn2 = createConnection();
        Connection conn3 = createConnection();
        
        try {
            // Execute queries on different connections
            List<List<Object>> results1 = executeQuery(conn1, "SELECT 1");
            List<List<Object>> results2 = executeQuery(conn2, "SELECT 2");
            List<List<Object>> results3 = executeQuery(conn3, "SELECT 3");
            
            assertThat(results1.get(0)).containsExactly(1);
            assertThat(results2.get(0)).containsExactly(2);
            assertThat(results3.get(0)).containsExactly(3);
        } finally {
            conn1.close();
            conn2.close();
            conn3.close();
        }
    }
}