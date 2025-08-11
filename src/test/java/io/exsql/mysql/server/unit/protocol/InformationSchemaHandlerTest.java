package io.exsql.mysql.server.unit.protocol;

import io.exsql.mysql.server.protocol.InformationSchemaHandler;
import io.exsql.mysql.server.util.SparkTestBase;
import io.exsql.mysql.server.util.TestDataGenerator;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.classic.Dataset;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for InformationSchemaHandler.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class InformationSchemaHandlerTest extends SparkTestBase {

    private static InformationSchemaHandler handler;

    @BeforeAll
    static void setupTestData() {
        handler = new InformationSchemaHandler(spark);
        
        // Create test databases and tables
        TestDataGenerator.registerAllTestTables(spark);
    }

    @Test
    @DisplayName("01. Test isInformationSchemaQuery detection")
    void testIsInformationSchemaQuery() {
        // Information schema queries
        assertThat(InformationSchemaHandler.isInformationSchemaQuery(
            "SELECT * FROM information_schema.TABLES")).isTrue();
        assertThat(InformationSchemaHandler.isInformationSchemaQuery(
            "SELECT * FROM information_schema.SCHEMATA")).isTrue();
        assertThat(InformationSchemaHandler.isInformationSchemaQuery(
            "SELECT * FROM information_schema.COLUMNS")).isTrue();
        
        // SHOW commands
        assertThat(InformationSchemaHandler.isInformationSchemaQuery("SHOW DATABASES")).isTrue();
        assertThat(InformationSchemaHandler.isInformationSchemaQuery("SHOW SCHEMAS")).isTrue();
        assertThat(InformationSchemaHandler.isInformationSchemaQuery("SHOW TABLES")).isTrue();
        assertThat(InformationSchemaHandler.isInformationSchemaQuery("SHOW COLUMNS FROM table1")).isTrue();
        assertThat(InformationSchemaHandler.isInformationSchemaQuery("show databases")).isTrue(); // case insensitive
        
        // Regular queries (should not match)
        assertThat(InformationSchemaHandler.isInformationSchemaQuery("SELECT * FROM users")).isFalse();
        assertThat(InformationSchemaHandler.isInformationSchemaQuery("UPDATE users SET name = 'test'")).isFalse();
        assertThat(InformationSchemaHandler.isInformationSchemaQuery("CREATE TABLE test (id INT)")).isFalse();
    }

    @Test
    @DisplayName("02. Test SHOW DATABASES command")
    void testShowDatabases() {
        Dataset<Row> result = handler.processQuery("SHOW DATABASES", "default");
        
        List<Row> rows = result.collectAsList();
        assertThat(rows).isNotEmpty();
        
        // Should contain at least information_schema, default, and test_db
        List<String> databaseNames = rows.stream()
            .map(row -> row.getString(0))
            .toList();
        
        assertThat(databaseNames).contains("information_schema", "default");
        
        // Verify column name
        assertThat(result.columns()).containsExactly("Database");
    }

    @Test
    @DisplayName("03. Test SHOW TABLES in default database")
    void testShowTablesDefault() {
        Dataset<Row> result = handler.processQuery("SHOW TABLES", "default");
        
        List<Row> rows = result.collectAsList();
        List<String> tableNames = rows.stream()
            .map(row -> row.getString(0))
            .toList();
        
        // Should contain the tables we created in default database
        assertThat(tableNames).contains("default_persons", "default_orders");
        
        // Verify column name includes database name
        assertThat(result.columns()).containsExactly("Tables_in_default");
    }

    @Test
    @DisplayName("04. Test SHOW TABLES in information_schema")
    void testShowTablesInformationSchema() {
        Dataset<Row> result = handler.processQuery("SHOW TABLES", "information_schema");
        
        List<Row> rows = result.collectAsList();
        List<String> tableNames = rows.stream()
            .map(row -> row.getString(0))
            .toList();
        
        // Should contain the standard information_schema tables
        assertThat(tableNames).contains("SCHEMATA", "TABLES", "COLUMNS");
        
        // Verify column name
        assertThat(result.columns()).containsExactly("Tables_in_information_schema");
    }

    @Test
    @DisplayName("05. Test SHOW COLUMNS from information_schema table")
    void testShowColumnsInformationSchema() {
        Dataset<Row> result = handler.processQuery("SHOW COLUMNS FROM SCHEMATA", "information_schema");
        
        List<Row> rows = result.collectAsList();
        assertThat(rows).hasSize(4); // SCHEMATA has 4 columns
        
        // Verify column structure
        assertThat(result.columns()).containsExactly("Field", "Type", "Null", "Key", "Default", "Extra");
        
        // Verify some expected column names
        List<String> fieldNames = rows.stream()
            .map(row -> row.getString(0))
            .toList();
        
        assertThat(fieldNames).contains("CATALOG_NAME", "SCHEMA_NAME", "DEFAULT_CHARACTER_SET_NAME", "DEFAULT_COLLATION_NAME");
    }

    @Test
    @DisplayName("06. Test SHOW COLUMNS from Spark table")
    void testShowColumnsSparkTable() {
        Dataset<Row> result = handler.processQuery("SHOW COLUMNS FROM default_persons", "default");
        
        List<Row> rows = result.collectAsList();
        assertThat(rows).hasSize(5); // persons table has 5 columns
        
        // Verify column structure
        assertThat(result.columns()).containsExactly("Field", "Type", "Null", "Key", "Default", "Extra");
        
        // Verify some expected column names
        List<String> fieldNames = rows.stream()
            .map(row -> row.getString(0))
            .toList();
        
        assertThat(fieldNames).contains("id", "name", "age", "email", "salary");
    }

    @Test
    @DisplayName("07. Test SELECT from information_schema.SCHEMATA")
    void testSelectSchemata() {
        Dataset<Row> result = handler.processQuery(
            "SELECT SCHEMA_NAME FROM information_schema.SCHEMATA", "information_schema");
        
        List<Row> rows = result.collectAsList();
        assertThat(rows).isNotEmpty();
        
        // Should contain information_schema and our test databases
        List<String> schemaNames = rows.stream()
            .map(row -> row.getString(1)) // SCHEMA_NAME is the second column
            .toList();
        
        assertThat(schemaNames).contains("information_schema", "default");
    }

    @Test
    @DisplayName("08. Test SELECT from information_schema.TABLES")
    void testSelectTables() {
        Dataset<Row> result = handler.processQuery(
            "SELECT TABLE_SCHEMA, TABLE_NAME FROM information_schema.TABLES", "information_schema");
        
        List<Row> rows = result.collectAsList();
        assertThat(rows).isNotEmpty();
        
        // Should contain both information_schema tables and Spark tables
        boolean hasInformationSchemaTables = rows.stream()
            .anyMatch(row -> "information_schema".equals(row.getString(1)) && 
                           Arrays.asList("SCHEMATA", "TABLES", "COLUMNS").contains(row.getString(2)));
        
        boolean hasSparkTables = rows.stream()
            .anyMatch(row -> "default".equals(row.getString(1)) && 
                           Arrays.asList("default_persons", "default_orders").contains(row.getString(2)));
        
        assertThat(hasInformationSchemaTables).isTrue();
        assertThat(hasSparkTables).isTrue();
    }

    @Test
    @DisplayName("09. Test SELECT from information_schema.COLUMNS")
    void testSelectColumns() {
        Dataset<Row> result = handler.processQuery(
            "SELECT TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS", "information_schema");
        
        List<Row> rows = result.collectAsList();
        assertThat(rows).isNotEmpty();
        
        // Should contain columns from both information_schema and Spark tables
        boolean hasInformationSchemaColumns = rows.stream()
            .anyMatch(row -> "information_schema".equals(row.getString(1)) && 
                           "SCHEMATA".equals(row.getString(2)) &&
                           "SCHEMA_NAME".equals(row.getString(3)));
        
        boolean hasSparkColumns = rows.stream()
            .anyMatch(row -> "default".equals(row.getString(1)) && 
                           "default_persons".equals(row.getString(2)) &&
                           Arrays.asList("id", "name", "age", "email", "salary").contains(row.getString(3)));
        
        assertThat(hasInformationSchemaColumns).isTrue();
        assertThat(hasSparkColumns).isTrue();
    }

    @Test
    @DisplayName("10. Test unhandled query returns empty result")
    void testUnhandledQuery() {
        Dataset<Row> result = handler.processQuery("SELECT * FROM some_unknown_table", "default");
        
        List<Row> rows = result.collectAsList();
        assertThat(rows).isEmpty();
        assertThat(result.columns()).isEmpty();
    }

    @Test
    @DisplayName("11. Test case insensitive query matching")
    void testCaseInsensitiveMatching() {
        // Test SHOW commands with different cases
        Dataset<Row> result1 = handler.processQuery("show databases", "default");
        Dataset<Row> result2 = handler.processQuery("SHOW DATABASES", "default");
        
        assertThat(result1.collectAsList()).hasSameSizeAs(result2.collectAsList());
        
        // Test information_schema queries with different cases
        Dataset<Row> result3 = handler.processQuery(
            "select * from information_schema.schemata", "information_schema");
        Dataset<Row> result4 = handler.processQuery(
            "SELECT * FROM INFORMATION_SCHEMA.SCHEMATA", "information_schema");
        
        assertThat(result3.collectAsList()).hasSameSizeAs(result4.collectAsList());
    }

    @Test
    @DisplayName("12. Test error handling for invalid table in SHOW COLUMNS")
    void testShowColumnsInvalidTable() {
        // This should not throw an exception, just return empty result
        Dataset<Row> result = handler.processQuery("SHOW COLUMNS FROM nonexistent_table", "default");
        
        List<Row> rows = result.collectAsList();
        assertThat(rows).isEmpty();
    }
}