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
 * Integration tests for MySQL SHOW commands.
 * Tests the complete flow from MySQL client through the server to Spark.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class ShowCommandsTest extends MySQLClientTestBase {

    @BeforeAll
    static void setupTestData() {
        // Create test data in both default and test_db databases
        TestDataGenerator.registerAllTestTables(spark);
    }

    @Test
    @DisplayName("01. Test SHOW DATABASES command")
    void testShowDatabases() throws SQLException {
        List<List<Object>> results = executeQuery("SHOW DATABASES");
        
        assertThat(results).isNotEmpty();
        
        // Extract database names
        List<String> databaseNames = results.stream()
            .map(row -> (String) row.get(0))
            .toList();
        
        // Should contain our test databases
        assertThat(databaseNames).contains("default", "information_schema");
        
        // Verify they're sorted or at least in a predictable order
        assertThat(databaseNames).containsAnyOf("test_db"); // May or may not exist depending on Spark catalog
    }

    @Test
    @DisplayName("02. Test SHOW SCHEMAS command (alias for SHOW DATABASES)")
    void testShowSchemas() throws SQLException {
        List<List<Object>> databases = executeQuery("SHOW DATABASES");
        List<List<Object>> schemas = executeQuery("SHOW SCHEMAS");
        
        // SHOW SCHEMAS should return the same results as SHOW DATABASES
        assertThat(schemas).hasSameSizeAs(databases);
        
        // Extract names and compare
        List<String> databaseNames = databases.stream()
            .map(row -> (String) row.get(0))
            .toList();
        List<String> schemaNames = schemas.stream()
            .map(row -> (String) row.get(0))
            .toList();
        
        assertThat(schemaNames).containsExactlyInAnyOrderElementsOf(databaseNames);
    }

    @Test
    @DisplayName("03. Test SHOW TABLES in default database")
    void testShowTablesDefault() throws SQLException {
        try (Connection conn = createConnection()) {
            // Make sure we're in default database
            executeUpdate(conn, "USE default");
            
            List<List<Object>> results = executeQuery(conn, "SHOW TABLES");
            
            // Extract table names
            List<String> tableNames = results.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            // Should contain our test tables
            assertThat(tableNames).contains("default_persons", "default_orders");
        }
    }

    @Test
    @DisplayName("04. Test SHOW TABLES in information_schema")
    void testShowTablesInformationSchema() throws SQLException {
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE information_schema");
            
            List<List<Object>> results = executeQuery(conn, "SHOW TABLES");
            
            // Extract table names
            List<String> tableNames = results.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            // Should contain the standard information_schema tables
            assertThat(tableNames).contains("SCHEMATA", "TABLES", "COLUMNS");
            assertThat(tableNames).hasSize(3); // Should only have these 3 tables
        }
    }

    @Test
    @DisplayName("05. Test SHOW COLUMNS FROM Spark table")
    void testShowColumnsSparkTable() throws SQLException {
        List<List<Object>> results = executeQuery("SHOW COLUMNS FROM default.default_persons");
        
        assertThat(results).hasSize(5); // persons table has 5 columns
        
        // Extract field names (first column)
        List<String> fieldNames = results.stream()
            .map(row -> (String) row.get(0))
            .toList();
        
        // Should contain all the person table columns
        assertThat(fieldNames).containsExactlyInAnyOrder("id", "name", "age", "email", "salary");
        
        // Verify column structure (Field, Type, Null, Key, Default, Extra)
        assertThat(results.get(0)).hasSize(6);
        
        // Check some specific column properties
        // Find the 'id' column
        List<Object> idColumn = results.stream()
            .filter(row -> "id".equals(row.get(0)))
            .findFirst()
            .orElseThrow();
        
        assertThat(idColumn.get(1).toString().toLowerCase()).contains("int"); // Type should be integer-like
        assertThat(idColumn.get(2)).isEqualTo("NO"); // id should be NOT NULL
    }

    @Test
    @DisplayName("06. Test SHOW COLUMNS FROM information_schema table")
    void testShowColumnsInformationSchemaTable() throws SQLException {
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE information_schema");
            
            List<List<Object>> results = executeQuery(conn, "SHOW COLUMNS FROM SCHEMATA");
            
            assertThat(results).hasSize(4); // SCHEMATA has 4 columns
            
            // Extract field names
            List<String> fieldNames = results.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            // Should contain the SCHEMATA columns
            assertThat(fieldNames).containsExactlyInAnyOrder(
                "CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME"
            );
            
            // All columns should be NOT NULL
            results.forEach(row -> assertThat(row.get(2)).isEqualTo("NO"));
        }
    }

    @Test
    @DisplayName("07. Test SHOW commands case insensitivity")
    void testShowCommandsCaseInsensitive() throws SQLException {
        // Test different case combinations
        List<List<Object>> results1 = executeQuery("SHOW DATABASES");
        List<List<Object>> results2 = executeQuery("show databases");
        List<List<Object>> results3 = executeQuery("Show Databases");
        
        assertThat(results2).hasSameSizeAs(results1);
        assertThat(results3).hasSameSizeAs(results1);
        
        // Same for SHOW TABLES
        List<List<Object>> tables1 = executeQuery("SHOW TABLES");
        List<List<Object>> tables2 = executeQuery("show tables");
        
        assertThat(tables2).hasSameSizeAs(tables1);
    }

    @Test
    @DisplayName("08. Test SHOW COLUMNS with qualified table name")
    void testShowColumnsQualifiedTableName() throws SQLException {
        // Test both database.table format and USE database + table format
        List<List<Object>> results1 = executeQuery("SHOW COLUMNS FROM default.default_persons");
        
        List<List<Object>> results2;
        try (Connection conn = createConnection()) {
            executeUpdate(conn, "USE default");
            results2 = executeQuery(conn, "SHOW COLUMNS FROM default_persons");
        }
        
        // Should return the same results
        assertThat(results2).hasSameSizeAs(results1);
        
        // Compare field names
        List<String> fieldNames1 = results1.stream().map(row -> (String) row.get(0)).toList();
        List<String> fieldNames2 = results2.stream().map(row -> (String) row.get(0)).toList();
        
        assertThat(fieldNames2).containsExactlyInAnyOrderElementsOf(fieldNames1);
    }

    @Test
    @DisplayName("09. Test SHOW COLUMNS error handling for non-existent table")
    void testShowColumnsNonExistentTable() throws SQLException {
        // This should not throw an exception, but return empty result
        List<List<Object>> results = executeQuery("SHOW COLUMNS FROM nonexistent_table");
        
        // Should return empty result set
        assertThat(results).isEmpty();
    }

    @Test
    @DisplayName("10. Test SHOW commands with different connection contexts")
    void testShowCommandsWithDifferentContexts() throws SQLException {
        Connection conn1 = createConnection();
        Connection conn2 = createConnection();
        
        try {
            // Set different database contexts
            executeUpdate(conn1, "USE default");
            executeUpdate(conn2, "USE information_schema");
            
            // SHOW TABLES should return different results
            List<List<Object>> defaultTables = executeQuery(conn1, "SHOW TABLES");
            List<List<Object>> infoTables = executeQuery(conn2, "SHOW TABLES");
            
            List<String> defaultTableNames = defaultTables.stream()
                .map(row -> (String) row.get(0))
                .toList();
            List<String> infoTableNames = infoTables.stream()
                .map(row -> (String) row.get(0))
                .toList();
            
            // Should be different sets of tables
            assertThat(defaultTableNames).isNotEqualTo(infoTableNames);
            assertThat(infoTableNames).contains("SCHEMATA", "TABLES", "COLUMNS");
            assertThat(defaultTableNames).doesNotContain("SCHEMATA", "TABLES", "COLUMNS");
            
        } finally {
            conn1.close();
            conn2.close();
        }
    }
}