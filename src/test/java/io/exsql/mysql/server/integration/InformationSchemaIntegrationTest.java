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
 * Integration tests for information_schema database queries.
 * Tests MySQL-compatible metadata access through real client connections.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class InformationSchemaIntegrationTest extends MySQLClientTestBase {

    @BeforeAll
    static void setupTestData() {
        // Create test data in multiple databases
        TestDataGenerator.registerAllTestTables(spark);
    }

    @Test
    @DisplayName("01. Test SELECT from information_schema.SCHEMATA")
    void testSelectSchemata() throws SQLException {
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE information_schema");
            
            List<List<Object>> results = executeQuery(conn, 
                "SELECT CATALOG_NAME, SCHEMA_NAME FROM SCHEMATA ORDER BY SCHEMA_NAME");
            
            assertThat(results).isNotEmpty();
            
            // Check that we have our expected databases
            List<String> schemaNames = results.stream()
                .map(row -> (String) row.get(1)) // SCHEMA_NAME is second column
                .toList();
            
            assertThat(schemaNames).contains("default", "information_schema");
            
            // All should have catalog name "def"
            results.forEach(row -> assertThat(row.get(0)).isEqualTo("def"));
        }
    }

    @Test
    @DisplayName("02. Test SELECT from information_schema.TABLES")
    void testSelectTables() throws SQLException {
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE information_schema");
            
            List<List<Object>> results = executeQuery(conn,
                "SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE FROM TABLES ORDER BY TABLE_SCHEMA, TABLE_NAME");
            
            assertThat(results).isNotEmpty();
            
            // Should have both information_schema and Spark tables
            boolean hasInformationSchemaTables = results.stream()
                .anyMatch(row -> "information_schema".equals(row.get(1)) && 
                               List.of("SCHEMATA", "TABLES", "COLUMNS").contains(row.get(2)));
            
            boolean hasSparkTables = results.stream()
                .anyMatch(row -> "default".equals(row.get(1)) && 
                               List.of("default_persons", "default_orders").contains(row.get(2)));
            
            assertThat(hasInformationSchemaTables).isTrue();
            assertThat(hasSparkTables).isTrue();
            
            // Verify table types
            List<Object> infoSchemaTable = results.stream()
                .filter(row -> "information_schema".equals(row.get(1)) && "SCHEMATA".equals(row.get(2)))
                .findFirst()
                .orElseThrow();
            assertThat(infoSchemaTable.get(3)).isEqualTo("SYSTEM VIEW");
            
            List<Object> sparkTable = results.stream()
                .filter(row -> "default".equals(row.get(1)) && "default_persons".equals(row.get(2)))
                .findFirst()
                .orElseThrow();
            assertThat(sparkTable.get(3)).isEqualTo("BASE TABLE");
        }
    }

    @Test
    @DisplayName("03. Test SELECT from information_schema.COLUMNS")
    void testSelectColumns() throws SQLException {
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE information_schema");
            
            List<List<Object>> results = executeQuery(conn,
                "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM COLUMNS WHERE TABLE_SCHEMA = 'default' AND TABLE_NAME = 'default_persons' ORDER BY ORDINAL_POSITION");
            
            assertThat(results).hasSize(5); // persons table has 5 columns
            
            // Verify column names and order
            List<String> columnNames = results.stream()
                .map(row -> (String) row.get(2)) // COLUMN_NAME
                .toList();
            
            assertThat(columnNames).containsExactly("id", "name", "age", "email", "salary");
            
            // Verify ordinal positions
            for (int i = 0; i < results.size(); i++) {
                assertThat(results.get(i).get(3)).isEqualTo((long)(i + 1)); // ORDINAL_POSITION
            }
        }
    }

    @Test
    @DisplayName("04. Test complex information_schema query with JOINs")
    void testComplexInformationSchemaQuery() throws SQLException {
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE information_schema");
            
            String sql = """
                SELECT s.SCHEMA_NAME, COUNT(t.TABLE_NAME) as table_count
                FROM SCHEMATA s
                LEFT JOIN TABLES t ON s.SCHEMA_NAME = t.TABLE_SCHEMA
                GROUP BY s.SCHEMA_NAME
                ORDER BY s.SCHEMA_NAME
                """;
            
            List<List<Object>> results = executeQuery(conn, sql);
            
            assertThat(results).isNotEmpty();
            
            // Find the default schema
            List<Object> defaultSchema = results.stream()
                .filter(row -> "default".equals(row.get(0)))
                .findFirst()
                .orElseThrow();
            
            // Should have at least 2 tables (default_persons, default_orders)
            long tableCount = (Long) defaultSchema.get(1);
            assertThat(tableCount).isGreaterThanOrEqualTo(2);
            
            // Find information_schema
            List<Object> infoSchema = results.stream()
                .filter(row -> "information_schema".equals(row.get(0)))
                .findFirst()
                .orElseThrow();
            
            // Should have exactly 3 tables (SCHEMATA, TABLES, COLUMNS)
            long infoTableCount = (Long) infoSchema.get(1);
            assertThat(infoTableCount).isEqualTo(3);
        }
    }

    @Test
    @DisplayName("05. Test information_schema queries with WHERE clauses")
    void testInformationSchemaWithWhere() throws SQLException {
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE information_schema");
            
            // Test filtering tables by schema
            List<List<Object>> results = executeQuery(conn,
                "SELECT TABLE_NAME FROM TABLES WHERE TABLE_SCHEMA = 'information_schema' ORDER BY TABLE_NAME");
            
            List<String> tableNames = results.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(tableNames).containsExactly("COLUMNS", "SCHEMATA", "TABLES");
            
            // Test filtering columns by table
            List<List<Object>> columnResults = executeQuery(conn,
                "SELECT COLUMN_NAME FROM COLUMNS WHERE TABLE_SCHEMA = 'default' AND TABLE_NAME = 'default_persons' ORDER BY ORDINAL_POSITION");
            
            List<String> columnNames = columnResults.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(columnNames).containsExactly("id", "name", "age", "email", "salary");
        }
    }

    @Test
    @DisplayName("06. Test information_schema access from different database contexts")
    void testInformationSchemaFromDifferentContexts() throws SQLException {
        // Test accessing information_schema tables from different database contexts
        
        // From default database
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE default");
            
            List<List<Object>> results = executeQuery(conn,
                "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME");
            
            List<String> schemaNames = results.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(schemaNames).contains("default", "information_schema");
        }
        
        // From information_schema database
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE information_schema");
            
            List<List<Object>> results = executeQuery(conn,
                "SELECT SCHEMA_NAME FROM SCHEMATA ORDER BY SCHEMA_NAME");
            
            List<String> schemaNames = results.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(schemaNames).contains("default", "information_schema");
        }
    }

    @Test
    @DisplayName("07. Test MySQL client tools compatibility queries")
    void testMySQLClientToolsCompatibility() throws SQLException {
        // Test common queries that MySQL client tools use
        
        try (Connection conn = createConnection()) {
            // Query that mysql command line client uses
            List<List<Object>> results1 = executeQuery(conn,
                "SELECT SCHEMA_NAME AS `Database` FROM information_schema.SCHEMATA ORDER BY SCHEMA_NAME");
            
            assertThat(results1).isNotEmpty();
            
            // Query that some GUI tools use to list tables
            List<List<Object>> results2 = executeQuery(conn,
                "SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = 'default' AND TABLE_TYPE = 'BASE TABLE'");
            
            List<String> tableNames = results2.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            assertThat(tableNames).contains("default_persons", "default_orders");
            
            // Query to get column information for a specific table
            List<List<Object>> results3 = executeQuery(conn,
                "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'default' AND TABLE_NAME = 'default_persons' ORDER BY ORDINAL_POSITION");
            
            assertThat(results3).hasSize(5);
        }
    }

    @Test
    @DisplayName("08. Test information_schema performance with larger datasets")
    void testInformationSchemaPerformance() throws SQLException {
        // Create a larger test table to verify performance
        spark.sql("CREATE OR REPLACE TEMPORARY VIEW large_test_table AS SELECT id, name FROM default.default_persons");
        
        try (Connection conn = createConnection()) {
            long startTime = System.currentTimeMillis();
            
            // Query that should be reasonably fast
            List<List<Object>> results = executeQuery(conn,
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'default'");
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            // Should complete within reasonable time (less than 5 seconds)
            assertThat(duration).isLessThan(5000);
            
            // Should return a reasonable count
            long columnCount = (Long) results.get(0).get(0);
            assertThat(columnCount).isGreaterThan(0);
        }
    }
}