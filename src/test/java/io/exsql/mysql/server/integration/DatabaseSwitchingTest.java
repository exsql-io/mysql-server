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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Integration tests for database switching functionality.
 * Tests both USE statements and COM_INIT_DB commands.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class DatabaseSwitchingTest extends MySQLClientTestBase {

    @BeforeAll
    static void setupTestData() {
        // Create test data in multiple databases
        TestDataGenerator.registerAllTestTables(spark);
    }

    @Test
    @DisplayName("01. Test basic USE statement")
    void testBasicUseStatement() throws SQLException {
        try (Connection conn = createConnection()) {
            // Start in default database
            List<List<Object>> tables1 = executeQuery(conn, "SHOW TABLES");
            List<String> defaultTables = tables1.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            // Switch to information_schema
            executeUpdate(conn, "USE information_schema");
            
            List<List<Object>> tables2 = executeQuery(conn, "SHOW TABLES");
            List<String> infoTables = tables2.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            // Should be different sets of tables
            assertThat(infoTables).isNotEqualTo(defaultTables);
            assertThat(infoTables).contains("SCHEMATA", "TABLES", "COLUMNS");
        }
    }

    @Test
    @DisplayName("02. Test USE statement with quoted database names")
    void testUseStatementWithQuotes() throws SQLException {
        try (Connection conn = createConnection()) {
            // Test different quote styles
            executeUpdate(conn, "USE `default`");
            executeUpdate(conn, "USE 'information_schema'");
            executeUpdate(conn, "USE \"default\"");
            
            // All should work without errors
            List<List<Object>> tables = executeQuery(conn, "SHOW TABLES");
            assertThat(tables).isNotEmpty();
        }
    }

    @Test
    @DisplayName("03. Test USE statement case insensitivity")
    void testUseStatementCaseInsensitive() throws SQLException {
        try (Connection conn = createConnection()) {
            // Test different cases
            executeUpdate(conn, "use default");
            executeUpdate(conn, "USE INFORMATION_SCHEMA");
            executeUpdate(conn, "Use Default");
            
            // All should work
            List<List<Object>> tables = executeQuery(conn, "SHOW TABLES");
            assertThat(tables).isNotEmpty();
        }
    }

    @Test
    @DisplayName("04. Test database context affects query results")
    void testDatabaseContextAffectsQueries() throws SQLException {
        try (Connection conn = createConnection()) {
            // In default database
            executeUpdate(conn, "USE default");
            List<List<Object>> defaultTables = executeQuery(conn, "SHOW TABLES");
            
            // In information_schema database
            executeUpdate(conn, "USE information_schema");
            List<List<Object>> infoTables = executeQuery(conn, "SHOW TABLES");
            
            // Should return different results
            assertThat(defaultTables).isNotEqualTo(infoTables);
            
            // Verify specific content
            List<String> defaultTableNames = defaultTables.stream()
                .map(row -> (String) row.get(0))
                .toList();
            List<String> infoTableNames = infoTables.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(defaultTableNames).contains("default_persons", "default_orders");
            assertThat(infoTableNames).contains("SCHEMATA", "TABLES", "COLUMNS");
        }
    }

    @Test
    @DisplayName("05. Test multiple database switches in same connection")
    void testMultipleDatabaseSwitches() throws SQLException {
        try (Connection conn = createConnection()) {
            // Switch between databases multiple times
            executeUpdate(conn, "USE default");
            assertRowCount(conn, "SHOW TABLES", 2); // Should have default_persons and default_orders
            
            executeUpdate(conn, "USE information_schema");
            assertRowCount(conn, "SHOW TABLES", 3); // Should have SCHEMATA, TABLES, COLUMNS
            
            executeUpdate(conn, "USE default");
            assertRowCount(conn, "SHOW TABLES", 2); // Back to default tables
            
            executeUpdate(conn, "USE information_schema");
            assertRowCount(conn, "SHOW TABLES", 3); // Back to info schema tables
        }
    }

    @Test
    @DisplayName("06. Test database context isolation between connections")
    void testDatabaseContextIsolation() throws SQLException {
        Connection conn1 = createConnection();
        Connection conn2 = createConnection();
        
        try {
            // Set different databases on each connection
            executeUpdate(conn1, "USE default");
            executeUpdate(conn2, "USE information_schema");
            
            // Each connection should see its own database's tables
            List<List<Object>> conn1Tables = executeQuery(conn1, "SHOW TABLES");
            List<List<Object>> conn2Tables = executeQuery(conn2, "SHOW TABLES");
            
            List<String> conn1TableNames = conn1Tables.stream()
                .map(row -> (String) row.get(0))
                .toList();
            List<String> conn2TableNames = conn2Tables.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(conn1TableNames).contains("default_persons", "default_orders");
            assertThat(conn2TableNames).contains("SCHEMATA", "TABLES", "COLUMNS");
            
            // Switch conn1 to information_schema - should not affect conn2
            executeUpdate(conn1, "USE information_schema");
            
            List<List<Object>> conn1TablesAfter = executeQuery(conn1, "SHOW TABLES");
            List<List<Object>> conn2TablesAfter = executeQuery(conn2, "SHOW TABLES");
            
            // conn1 should now see info schema tables
            List<String> conn1TableNamesAfter = conn1TablesAfter.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(conn1TableNamesAfter).contains("SCHEMATA", "TABLES", "COLUMNS");
            
            // conn2 should still see the same tables
            assertThat(conn2TablesAfter).hasSameSizeAs(conn2Tables);
            
        } finally {
            conn1.close();
            conn2.close();
        }
    }

    @Test
    @DisplayName("07. Test qualified table names bypass database context")
    void testQualifiedTableNamesBypassContext() throws SQLException {
        try (Connection conn = createConnection()) {
            // Set context to information_schema
            executeUpdate(conn, "USE information_schema");
            
            // Should still be able to query default database tables with qualified names
            List<List<Object>> results = executeQuery(conn, "SELECT COUNT(*) FROM default.default_persons");
            
            assertThat(results).hasSize(1);
            Long count = (Long) results.get(0).get(0);
            assertThat(count).isGreaterThan(0);
            
            // And vice versa - from default database, query information_schema
            executeUpdate(conn, "USE default");
            
            List<List<Object>> schemaResults = executeQuery(conn, 
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = 'default'");
            
            assertThat(schemaResults).hasSize(1);
            assertThat(schemaResults.get(0).get(0)).isEqualTo("default");
        }
    }

    @Test
    @DisplayName("08. Test USE with non-existent database")
    void testUseNonExistentDatabase() throws SQLException {
        try (Connection conn = createConnection()) {
            // This should fail
            assertThatThrownBy(() -> executeUpdate(conn, "USE nonexistent_database"))
                .isInstanceOf(SQLException.class)
                .hasMessageContaining("Unknown database");
        }
    }

    @Test
    @DisplayName("09. Test database switching affects SHOW COLUMNS context")
    void testDatabaseSwitchingAffectsShowColumns() throws SQLException {
        try (Connection conn = createConnection()) {
            // In default database
            executeUpdate(conn, "USE default");
            
            // SHOW COLUMNS without database qualifier should work for local tables
            List<List<Object>> columns1 = executeQuery(conn, "SHOW COLUMNS FROM default_persons");
            assertThat(columns1).hasSize(5); // persons table has 5 columns
            
            // Switch to information_schema
            executeUpdate(conn, "USE information_schema");
            
            // Now SHOW COLUMNS should work for information_schema tables
            List<List<Object>> columns2 = executeQuery(conn, "SHOW COLUMNS FROM SCHEMATA");
            assertThat(columns2).hasSize(4); // SCHEMATA table has 4 columns
            
            // But should fail for default tables without qualifier
            List<List<Object>> columns3 = executeQuery(conn, "SHOW COLUMNS FROM default_persons");
            assertThat(columns3).isEmpty(); // Should not find the table
        }
    }

    @Test
    @DisplayName("10. Test database switching with transactions")
    void testDatabaseSwitchingWithTransactions() throws SQLException {
        try (Connection conn = createConnection()) {
            conn.setAutoCommit(false);
            
            try {
                executeUpdate(conn, "USE default");
                
                // Verify we're in the right database
                List<List<Object>> tables1 = executeQuery(conn, "SHOW TABLES");
                List<String> tableNames1 = tables1.stream()
                    .map(row -> (String) row.get(0))
                    .toList();
                assertThat(tableNames1).contains("default_persons");
                
                executeUpdate(conn, "USE information_schema");
                
                // Verify database switch worked
                List<List<Object>> tables2 = executeQuery(conn, "SHOW TABLES");
                List<String> tableNames2 = tables2.stream()
                    .map(row -> (String) row.get(0))
                    .toList();
                assertThat(tableNames2).contains("SCHEMATA");
                
                conn.commit();
                
                // After commit, should still be in information_schema
                List<List<Object>> tables3 = executeQuery(conn, "SHOW TABLES");
                List<String> tableNames3 = tables3.stream()
                    .map(row -> (String) row.get(0))
                    .toList();
                assertThat(tableNames3).contains("SCHEMATA");
                
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    // Helper method for checking row count on a specific connection
    private void assertRowCount(Connection conn, String sql, int expectedCount) throws SQLException {
        List<List<Object>> results = executeQuery(conn, sql);
        assertThat(results).hasSize(expectedCount);
    }
}