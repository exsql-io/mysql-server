package io.exsql.mysql.server.util;

import com.google.common.collect.Lists;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.classic.Dataset;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Utility class for generating test data in Spark DataFrames.
 * Provides various pre-defined datasets for testing different scenarios.
 */
public class TestDataGenerator {

    private static final Random random = new Random(42); // Fixed seed for reproducible tests

    /**
     * Create a simple persons table with basic data types.
     */
    public static Dataset<Row> createPersonsTable(SparkSession spark, int numRows) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("email", DataTypes.StringType, true),
                DataTypes.createStructField("salary", DataTypes.DoubleType, true)
        });

        List<Row> rows = new ArrayList<>();
        for (int i = 1; i <= numRows; i++) {
            rows.add(new GenericRow(new Object[]{
                    i,
                    "Person " + i,
                    20 + random.nextInt(60), // Age between 20-80
                    "person" + i + "@example.com",
                    30000.0 + random.nextDouble() * 70000.0 // Salary between 30K-100K
            }));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Create an orders table related to persons.
     */
    public static Dataset<Row> createOrdersTable(SparkSession spark, int numRows) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("order_id", DataTypes.LongType, false),
                DataTypes.createStructField("person_id", DataTypes.IntegerType, false),
                DataTypes.createStructField("product", DataTypes.StringType, false),
                DataTypes.createStructField("quantity", DataTypes.IntegerType, false),
                DataTypes.createStructField("price", DataTypes.createDecimalType(10, 2), false),
                DataTypes.createStructField("order_date", DataTypes.DateType, false)
        });

        List<Row> rows = new ArrayList<>();
        String[] products = {"Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse"};
        
        for (int i = 1; i <= numRows; i++) {
            rows.add(new GenericRow(new Object[]{
                    (long) i,
                    1 + random.nextInt(Math.min(100, numRows)), // Random person_id
                    products[random.nextInt(products.length)],
                    1 + random.nextInt(10), // Quantity 1-10
                    BigDecimal.valueOf(10.0 + random.nextDouble() * 1000.0).setScale(2, BigDecimal.ROUND_HALF_UP),
                    Date.valueOf(LocalDate.now().minusDays(random.nextInt(365))) // Random date within last year
            }));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Create a table with Unicode and special characters.
     */
    public static Dataset<Row> createUnicodeTable(SparkSession spark) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("description", DataTypes.StringType, true),
                DataTypes.createStructField("emoji", DataTypes.StringType, true)
        });

        List<Row> rows = Lists.newArrayList(
                new GenericRow(new Object[]{1, "English", "Simple ASCII text", "😀"}),
                new GenericRow(new Object[]{2, "中文", "Chinese characters: 你好世界", "🇨🇳"}),
                new GenericRow(new Object[]{3, "العربية", "Arabic text: مرحبا بالعالم", "🌍"}),
                new GenericRow(new Object[]{4, "Русский", "Russian text: Привет мир", "🇷🇺"}),
                new GenericRow(new Object[]{5, "Español", "Spanish text: Hola mundo", "🇪🇸"}),
                new GenericRow(new Object[]{6, "日本語", "Japanese text: こんにちは世界", "🇯🇵"}),
                new GenericRow(new Object[]{7, "Français", "French text: Bonjour le monde", "🇫🇷"}),
                new GenericRow(new Object[]{8, "हिन्दी", "Hindi text: नमस्ते दुनिया", "🇮🇳"}),
                new GenericRow(new Object[]{9, "Special", "Special chars: @#$%^&*()_+-=[]{}|;':\",./<>?", "⚡"})
        );

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Create a table with all supported data types.
     */
    public static Dataset<Row> createAllDataTypesTable(SparkSession spark) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("bool_col", DataTypes.BooleanType, true),
                DataTypes.createStructField("byte_col", DataTypes.ByteType, true),
                DataTypes.createStructField("short_col", DataTypes.ShortType, true),
                DataTypes.createStructField("int_col", DataTypes.IntegerType, true),
                DataTypes.createStructField("long_col", DataTypes.LongType, true),
                DataTypes.createStructField("float_col", DataTypes.FloatType, true),
                DataTypes.createStructField("double_col", DataTypes.DoubleType, true),
                DataTypes.createStructField("decimal_col", DataTypes.createDecimalType(18, 4), true),
                DataTypes.createStructField("string_col", DataTypes.StringType, true),
                DataTypes.createStructField("date_col", DataTypes.DateType, true),
                DataTypes.createStructField("timestamp_col", DataTypes.TimestampType, true),
                DataTypes.createStructField("binary_col", DataTypes.BinaryType, true)
        });

        List<Row> rows = Lists.newArrayList(
                new GenericRow(new Object[]{
                        1, true, (byte) 127, (short) 32767, 2147483647, 9223372036854775807L,
                        3.14f, 2.718281828459045, BigDecimal.valueOf(123456.7890),
                        "Sample text", Date.valueOf("2024-01-15"),
                        Timestamp.valueOf(LocalDateTime.of(2024, 1, 15, 10, 30, 45)),
                        "Binary data".getBytes()
                }),
                new GenericRow(new Object[]{
                        2, false, (byte) -128, (short) -32768, -2147483648, -9223372036854775808L,
                        -3.14f, -2.718281828459045, BigDecimal.valueOf(-123456.7890),
                        "Another sample", Date.valueOf("2023-12-25"),
                        Timestamp.valueOf(LocalDateTime.of(2023, 12, 25, 23, 59, 59)),
                        "More binary".getBytes()
                }),
                new GenericRow(new Object[]{
                        3, null, null, null, null, null,
                        null, null, null,
                        null, null, null, null
                })
        );

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Create an empty table with a specific schema.
     */
    public static Dataset<Row> createEmptyTable(SparkSession spark) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false),
                DataTypes.createStructField("value", DataTypes.DoubleType, true)
        });

        return spark.createDataFrame(Lists.newArrayList(), schema);
    }

    /**
     * Create a table with a single row and single column.
     */
    public static Dataset<Row> createSingleValueTable(SparkSession spark, String columnName, Object value) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField(columnName, getDataTypeForValue(value), true)
        });

        List<Row> rows = Lists.newArrayList(new GenericRow(new Object[]{value}));
        return spark.createDataFrame(rows, schema);
    }

    /**
     * Create a large table for performance testing.
     */
    public static Dataset<Row> createLargeTable(SparkSession spark, int numRows) {
        StructType schema = new StructType(new StructField[]{
                DataTypes.createStructField("id", DataTypes.LongType, false),
                DataTypes.createStructField("data1", DataTypes.StringType, false),
                DataTypes.createStructField("data2", DataTypes.IntegerType, false),
                DataTypes.createStructField("data3", DataTypes.DoubleType, false),
                DataTypes.createStructField("data4", DataTypes.TimestampType, false)
        });

        List<Row> rows = new ArrayList<>();
        for (long i = 1; i <= numRows; i++) {
            rows.add(new GenericRow(new Object[]{
                    i,
                    "Data row " + i + " with some additional text to make it longer",
                    random.nextInt(1000000),
                    random.nextDouble() * 1000000.0,
                    Timestamp.valueOf(LocalDateTime.now().minusSeconds(random.nextInt(86400)))
            }));
        }

        return spark.createDataFrame(rows, schema);
    }

    /**
     * Register all test tables in the Spark catalog.
     */
    public static void registerAllTestTables(SparkSession spark) {
        // Create test database
        spark.sql("CREATE DATABASE IF NOT EXISTS test_db");
        spark.sql("USE test_db");

        // Register tables
        createPersonsTable(spark, 100).createOrReplaceTempView("persons");
        createOrdersTable(spark, 200).createOrReplaceTempView("orders");
        createUnicodeTable(spark).createOrReplaceTempView("unicode_test");
        createAllDataTypesTable(spark).createOrReplaceTempView("all_types");
        createEmptyTable(spark).createOrReplaceTempView("empty_table");
        createSingleValueTable(spark, "result", 42).createOrReplaceTempView("single_value");

        // Also create tables in default database
        spark.sql("USE default");
        createPersonsTable(spark, 50).createOrReplaceTempView("default_persons");
        createOrdersTable(spark, 100).createOrReplaceTempView("default_orders");
    }

    /**
     * Helper method to determine Spark data type for a given value.
     */
    private static org.apache.spark.sql.types.DataType getDataTypeForValue(Object value) {
        if (value == null) return DataTypes.StringType;
        if (value instanceof Boolean) return DataTypes.BooleanType;
        if (value instanceof Byte) return DataTypes.ByteType;
        if (value instanceof Short) return DataTypes.ShortType;
        if (value instanceof Integer) return DataTypes.IntegerType;
        if (value instanceof Long) return DataTypes.LongType;
        if (value instanceof Float) return DataTypes.FloatType;
        if (value instanceof Double) return DataTypes.DoubleType;
        if (value instanceof BigDecimal) return DataTypes.createDecimalType();
        if (value instanceof String) return DataTypes.StringType;
        if (value instanceof Date) return DataTypes.DateType;
        if (value instanceof Timestamp) return DataTypes.TimestampType;
        if (value instanceof byte[]) return DataTypes.BinaryType;
        return DataTypes.StringType; // Default fallback
    }
}