package io.exsql.mysql.server.protocol;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.classic.Dataset;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Handles MySQL information_schema database queries by providing metadata
 * about Spark catalogs, databases, tables, and columns in MySQL-compatible format.
 * 
 * The information_schema is a virtual database that provides access to database
 * metadata in a standardized way that MySQL clients expect.
 */
public class InformationSchemaHandler {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(InformationSchemaHandler.class);
    
    private final SparkSession spark;
    
    // Common MySQL information_schema table patterns
    private static final Pattern SCHEMATA_PATTERN = Pattern.compile(
        "SELECT\\s+.*\\s+FROM\\s+SCHEMATA", Pattern.CASE_INSENSITIVE);
    private static final Pattern TABLES_PATTERN = Pattern.compile(
        "SELECT\\s+.*\\s+FROM\\s+TABLES", Pattern.CASE_INSENSITIVE);
    private static final Pattern COLUMNS_PATTERN = Pattern.compile(
        "SELECT\\s+.*\\s+FROM\\s+COLUMNS", Pattern.CASE_INSENSITIVE);
    
    // SHOW command patterns
    private static final Pattern SHOW_DATABASES_PATTERN = Pattern.compile(
        "SHOW\\s+(?:DATABASES|SCHEMAS)", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_TABLES_PATTERN = Pattern.compile(
        "SHOW\\s+TABLES", Pattern.CASE_INSENSITIVE);
    private static final Pattern SHOW_COLUMNS_PATTERN = Pattern.compile(
        "SHOW\\s+COLUMNS\\s+FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);
    
    public InformationSchemaHandler(SparkSession spark) {
        this.spark = spark;
    }
    
    /**
     * Check if a query is targeting the information_schema database or is a SHOW command.
     */
    public static boolean isInformationSchemaQuery(String query) {
        String normalized = query.trim().toLowerCase();
        return normalized.contains("information_schema") || 
               normalized.contains("schemata") ||
               normalized.contains("from tables") ||
               normalized.contains("from columns") ||
               normalized.startsWith("show databases") ||
               normalized.startsWith("show schemas") ||
               normalized.startsWith("show tables") ||
               normalized.startsWith("show columns");
    }
    
    /**
     * Process an information_schema query and return appropriate dataset.
     */
    public Dataset<Row> processQuery(String query, String currentDatabase) {
        String normalizedQuery = query.trim().toLowerCase();
        
        LOGGER.debug("Processing information_schema/SHOW query: {}", query);
        
        try {
            // Handle SHOW commands first
            if (SHOW_DATABASES_PATTERN.matcher(query).find()) {
                return createShowDatabasesDataset();
            } else if (SHOW_TABLES_PATTERN.matcher(query).find()) {
                return createShowTablesDataset(currentDatabase);
            } else if (SHOW_COLUMNS_PATTERN.matcher(query).find()) {
                var matcher = SHOW_COLUMNS_PATTERN.matcher(query);
                if (matcher.find()) {
                    String tableName = matcher.group(1);
                    return createShowColumnsDataset(tableName, currentDatabase);
                }
            }
            
            // Handle information_schema table queries
            if (SCHEMATA_PATTERN.matcher(query).find() || normalizedQuery.contains("schemata")) {
                return createSchemataDataset();
            } else if (TABLES_PATTERN.matcher(query).find() || normalizedQuery.contains("from tables")) {
                return createTablesDataset();
            } else if (COLUMNS_PATTERN.matcher(query).find() || normalizedQuery.contains("from columns")) {
                return createColumnsDataset();
            } else {
                // Default fallback - return empty result
                LOGGER.warn("Unhandled information_schema/SHOW query: {}", query);
                return createEmptyDataset();
            }
        } catch (Exception e) {
            LOGGER.error("Error processing information_schema/SHOW query: {}", query, e);
            throw new RuntimeException("Error processing information_schema/SHOW query", e);
        }
    }
    
    /**
     * Create SCHEMATA table dataset (database list).
     */
    private Dataset<Row> createSchemataDataset() {
        var schema = StructType.fromDDL(
            "CATALOG_NAME string, SCHEMA_NAME string, DEFAULT_CHARACTER_SET_NAME string, DEFAULT_COLLATION_NAME string"
        );
        
        List<Row> rows = new ArrayList<>();
        
        // Add information_schema itself
        rows.add(new GenericRow(new Object[]{"def", "information_schema", "utf8", "utf8_general_ci"}));
        
        // Add Spark databases
        var databases = spark.catalog().listDatabases().collectAsList();
        for (var database : databases) {
            rows.add(new GenericRow(new Object[]{"def", database.name(), "utf8", "utf8_general_ci"}));
        }
        
        Encoder<Row> encoder = Encoders.row(schema);
        return spark.createDataset(rows, encoder);
    }
    
    /**
     * Create TABLES table dataset (table list).
     */
    private Dataset<Row> createTablesDataset() {
        var schema = StructType.fromDDL(
            "TABLE_CATALOG string, TABLE_SCHEMA string, TABLE_NAME string, TABLE_TYPE string, ENGINE string, VERSION bigint, ROW_FORMAT string, TABLE_ROWS bigint, AVG_ROW_LENGTH bigint, DATA_LENGTH bigint, MAX_DATA_LENGTH bigint, INDEX_LENGTH bigint, DATA_FREE bigint, AUTO_INCREMENT bigint, CREATE_TIME timestamp, UPDATE_TIME timestamp, CHECK_TIME timestamp, TABLE_COLLATION string, CHECKSUM bigint, CREATE_OPTIONS string, TABLE_COMMENT string"
        );
        
        List<Row> rows = new ArrayList<>();
        
        // Add information_schema tables
        rows.add(createTableRow("def", "information_schema", "SCHEMATA", "SYSTEM VIEW"));
        rows.add(createTableRow("def", "information_schema", "TABLES", "SYSTEM VIEW"));
        rows.add(createTableRow("def", "information_schema", "COLUMNS", "SYSTEM VIEW"));
        
        // Add Spark tables from all databases
        var databases = spark.catalog().listDatabases().collectAsList();
        for (var database : databases) {
            try {
                var tables = spark.catalog().listTables(database.name()).collectAsList();
                for (var table : tables) {
                    rows.add(createTableRow("def", database.name(), table.name(), "BASE TABLE"));
                }
            } catch (Exception e) {
                LOGGER.warn("Error listing tables for database {}: {}", database.name(), e.getMessage());
            }
        }
        
        Encoder<Row> encoder = Encoders.row(schema);
        return spark.createDataset(rows, encoder);
    }
    
    /**
     * Create COLUMNS table dataset (column list).
     */
    private Dataset<Row> createColumnsDataset() {
        var schema = StructType.fromDDL(
            "TABLE_CATALOG string, TABLE_SCHEMA string, TABLE_NAME string, COLUMN_NAME string, ORDINAL_POSITION bigint, COLUMN_DEFAULT string, IS_NULLABLE string, DATA_TYPE string, CHARACTER_MAXIMUM_LENGTH bigint, CHARACTER_OCTET_LENGTH bigint, NUMERIC_PRECISION bigint, NUMERIC_SCALE bigint, DATETIME_PRECISION bigint, CHARACTER_SET_NAME string, COLLATION_NAME string, COLUMN_TYPE string, COLUMN_KEY string, EXTRA string, PRIVILEGES string, COLUMN_COMMENT string"
        );
        
        List<Row> rows = new ArrayList<>();
        
        // Add columns for information_schema tables
        addInformationSchemaColumns(rows);
        
        // Add columns for Spark tables
        var databases = spark.catalog().listDatabases().collectAsList();
        for (var database : databases) {
            try {
                var tables = spark.catalog().listTables(database.name()).collectAsList();
                for (var table : tables) {
                    try {
                        var tableDF = spark.table(database.name() + "." + table.name());
                        var tableSchema = tableDF.schema();
                        
                        for (int i = 0; i < tableSchema.fields().length; i++) {
                            var field = tableSchema.fields()[i];
                            rows.add(createColumnRow("def", database.name(), table.name(), field.name(), i + 1, field.dataType().typeName()));
                        }
                    } catch (Exception e) {
                        LOGGER.warn("Error getting schema for table {}.{}: {}", database.name(), table.name(), e.getMessage());
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Error listing tables for database {}: {}", database.name(), e.getMessage());
            }
        }
        
        Encoder<Row> encoder = Encoders.row(schema);
        return spark.createDataset(rows, encoder);
    }
    
    /**
     * Create a table row for the TABLES information_schema table.
     */
    private Row createTableRow(String catalog, String schema, String tableName, String tableType) {
        return new GenericRow(new Object[]{
            catalog, schema, tableName, tableType, "SPARK", null, "Dynamic", null, null, null, null, null, null, null, null, null, null, "utf8_general_ci", null, "", ""
        });
    }
    
    /**
     * Create a column row for the COLUMNS information_schema table.
     */
    private Row createColumnRow(String catalog, String schema, String tableName, String columnName, int position, String dataType) {
        return new GenericRow(new Object[]{
            catalog, schema, tableName, columnName, (long)position, null, "YES", dataType, null, null, null, null, null, "utf8", "utf8_general_ci", dataType, "", "", "select,insert,update,references", ""
        });
    }
    
    /**
     * Add column definitions for information_schema tables themselves.
     */
    private void addInformationSchemaColumns(List<Row> rows) {
        // SCHEMATA columns
        rows.add(createColumnRow("def", "information_schema", "SCHEMATA", "CATALOG_NAME", 1, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "SCHEMATA", "SCHEMA_NAME", 2, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "SCHEMATA", "DEFAULT_CHARACTER_SET_NAME", 3, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "SCHEMATA", "DEFAULT_COLLATION_NAME", 4, "varchar"));
        
        // TABLES columns  
        rows.add(createColumnRow("def", "information_schema", "TABLES", "TABLE_CATALOG", 1, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "TABLES", "TABLE_SCHEMA", 2, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "TABLES", "TABLE_NAME", 3, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "TABLES", "TABLE_TYPE", 4, "varchar"));
        
        // COLUMNS columns
        rows.add(createColumnRow("def", "information_schema", "COLUMNS", "TABLE_CATALOG", 1, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "COLUMNS", "TABLE_SCHEMA", 2, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "COLUMNS", "TABLE_NAME", 3, "varchar"));
        rows.add(createColumnRow("def", "information_schema", "COLUMNS", "COLUMN_NAME", 4, "varchar"));
    }
    
    /**
     * Create an empty dataset for unsupported queries.
     */
    private Dataset<Row> createEmptyDataset() {
        var schema = new StructType(new org.apache.spark.sql.types.StructField[0]);
        return spark.createDataFrame(Lists.newArrayList(), schema);
    }
    
    /**
     * Create dataset for SHOW DATABASES command.
     */
    private Dataset<Row> createShowDatabasesDataset() {
        var schema = StructType.fromDDL("Database string");
        
        List<Row> rows = new ArrayList<>();
        
        // Add information_schema
        rows.add(new GenericRow(new Object[]{"information_schema"}));
        
        // Add Spark databases
        var databases = spark.catalog().listDatabases().collectAsList();
        for (var database : databases) {
            rows.add(new GenericRow(new Object[]{database.name()}));
        }
        
        Encoder<Row> encoder = Encoders.row(schema);
        return spark.createDataset(rows, encoder);
    }
    
    /**
     * Create dataset for SHOW TABLES command.
     */
    private Dataset<Row> createShowTablesDataset(String currentDatabase) {
        var schema = StructType.fromDDL("Tables_in_" + currentDatabase + " string");
        
        List<Row> rows = new ArrayList<>();
        
        try {
            if ("information_schema".equalsIgnoreCase(currentDatabase)) {
                // Show information_schema tables
                rows.add(new GenericRow(new Object[]{"SCHEMATA"}));
                rows.add(new GenericRow(new Object[]{"TABLES"}));
                rows.add(new GenericRow(new Object[]{"COLUMNS"}));
            } else {
                // Show Spark tables for current database
                var tables = spark.catalog().listTables(currentDatabase).collectAsList();
                for (var table : tables) {
                    rows.add(new GenericRow(new Object[]{table.name()}));
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Error listing tables for current database: {}", e.getMessage());
        }
        
        Encoder<Row> encoder = Encoders.row(schema);
        return spark.createDataset(rows, encoder);
    }
    
    /**
     * Create dataset for SHOW COLUMNS command.
     */
    private Dataset<Row> createShowColumnsDataset(String tableName, String currentDatabase) {
        var schema = StructType.fromDDL(
            "Field string, Type string, Null string, Key string, Default string, Extra string"
        );
        
        List<Row> rows = new ArrayList<>();
        
        try {
            if ("information_schema".equalsIgnoreCase(currentDatabase)) {
                // Handle information_schema table columns
                addInformationSchemaTableColumns(rows, tableName.toUpperCase());
            } else {
                // Handle Spark table columns
                var tableDF = spark.table(currentDatabase + "." + tableName);
                var tableSchema = tableDF.schema();
                
                for (var field : tableSchema.fields()) {
                    String nullable = field.nullable() ? "YES" : "NO";
                    rows.add(new GenericRow(new Object[]{
                        field.name(), 
                        field.dataType().typeName(), 
                        nullable, 
                        "", 
                        null, 
                        ""
                    }));
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Error getting columns for table {}: {}", tableName, e.getMessage());
        }
        
        Encoder<Row> encoder = Encoders.row(schema);
        return spark.createDataset(rows, encoder);
    }
    
    /**
     * Add column definitions for specific information_schema tables in SHOW COLUMNS format.
     */
    private void addInformationSchemaTableColumns(List<Row> rows, String tableName) {
        switch (tableName) {
            case "SCHEMATA":
                rows.add(new GenericRow(new Object[]{"CATALOG_NAME", "varchar(512)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"SCHEMA_NAME", "varchar(64)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"DEFAULT_CHARACTER_SET_NAME", "varchar(32)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"DEFAULT_COLLATION_NAME", "varchar(32)", "NO", "", null, ""}));
                break;
            case "TABLES":
                rows.add(new GenericRow(new Object[]{"TABLE_CATALOG", "varchar(512)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"TABLE_SCHEMA", "varchar(64)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"TABLE_NAME", "varchar(64)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"TABLE_TYPE", "varchar(64)", "NO", "", null, ""}));
                break;
            case "COLUMNS":
                rows.add(new GenericRow(new Object[]{"TABLE_CATALOG", "varchar(512)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"TABLE_SCHEMA", "varchar(64)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"TABLE_NAME", "varchar(64)", "NO", "", null, ""}));
                rows.add(new GenericRow(new Object[]{"COLUMN_NAME", "varchar(64)", "NO", "", null, ""}));
                break;
            default:
                // Unknown table
                break;
        }
    }
    
}