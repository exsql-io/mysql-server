package io.exsql.mysql.server.protocol.server;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class to convert Spark DataFrame/Dataset<Row> to COMQueryResponseBuilder.
 * This class serves as glue code between Spark's DataFrame API and MySQL protocol's response builder.
 * 
 * <p>Usage example:</p>
 * <pre>
 * // Create a Spark DataFrame
 * Dataset<Row> dataFrame = spark.sql("SELECT id, name FROM users");
 * 
 * // Convert it to a COMQueryResponseBuilder
 * COMQueryResponseBuilder builder = SparkCOMQueryResponseBuilder.fromDataset(dataFrame);
 * 
 * // Build the response packet
 * ByteBuf buffer = Unpooled.buffer();
 * builder.build(buffer);
 * 
 * // Send the buffer to the client
 * ctx.writeAndFlush(buffer);
 * </pre>
 * 
 * <p>The converter handles mapping of Spark data types to MySQL column types:</p>
 * <ul>
 *   <li>StringType → MYSQL_TYPE_VAR_STRING</li>
 *   <li>IntegerType → MYSQL_TYPE_LONG</li>
 *   <li>LongType → MYSQL_TYPE_LONGLONG</li>
 *   <li>ShortType → MYSQL_TYPE_SHORT</li>
 *   <li>ByteType → MYSQL_TYPE_TINY</li>
 *   <li>FloatType → MYSQL_TYPE_FLOAT</li>
 *   <li>DoubleType → MYSQL_TYPE_DOUBLE</li>
 *   <li>DecimalType → MYSQL_TYPE_NEWDECIMAL</li>
 *   <li>BooleanType → MYSQL_TYPE_TINY</li>
 *   <li>TimestampType → MYSQL_TYPE_TIMESTAMP</li>
 *   <li>DateType → MYSQL_TYPE_DATE</li>
 *   <li>BinaryType → MYSQL_TYPE_BLOB</li>
 *   <li>Complex types (Array, Map, Struct) → MYSQL_TYPE_VAR_STRING (as JSON)</li>
 * </ul>
 */
public final class COMQueryResponseDatasetHelper {

    // MySQL column type constants based on MySQL protocol specification
    // https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html
    private static final byte MYSQL_TYPE_DECIMAL = 0x00;
    private static final byte MYSQL_TYPE_TINY = 0x01;
    private static final byte MYSQL_TYPE_SHORT = 0x02;
    private static final byte MYSQL_TYPE_LONG = 0x03;
    private static final byte MYSQL_TYPE_FLOAT = 0x04;
    private static final byte MYSQL_TYPE_DOUBLE = 0x05;
    private static final byte MYSQL_TYPE_NULL = 0x06;
    private static final byte MYSQL_TYPE_TIMESTAMP = 0x07;
    private static final byte MYSQL_TYPE_LONGLONG = 0x08;
    private static final byte MYSQL_TYPE_INT24 = 0x09;
    private static final byte MYSQL_TYPE_DATE = 0x0A;
    private static final byte MYSQL_TYPE_TIME = 0x0B;
    private static final byte MYSQL_TYPE_DATETIME = 0x0C;
    private static final byte MYSQL_TYPE_YEAR = 0x0D;
    private static final byte MYSQL_TYPE_VARCHAR = 0x0F;
    private static final byte MYSQL_TYPE_BIT = (byte) 0x10;
    private static final byte MYSQL_TYPE_JSON = (byte) 0xF5;
    private static final byte MYSQL_TYPE_NEWDECIMAL = (byte) 0xF6;
    private static final byte MYSQL_TYPE_ENUM = (byte) 0xF7;
    private static final byte MYSQL_TYPE_SET = (byte) 0xF8;
    private static final byte MYSQL_TYPE_TINY_BLOB = (byte) 0xF9;
    private static final byte MYSQL_TYPE_MEDIUM_BLOB = (byte) 0xFA;
    private static final byte MYSQL_TYPE_LONG_BLOB = (byte) 0xFB;
    private static final byte MYSQL_TYPE_BLOB = (byte) 0xFC;
    private static final byte MYSQL_TYPE_VAR_STRING = (byte) 0xFD;
    private static final byte MYSQL_TYPE_STRING = (byte) 0xFE;
    private static final byte MYSQL_TYPE_GEOMETRY = (byte) 0xFF;

    // Default character set (utf8mb4)
    private static final int DEFAULT_CHARACTER_SET = 45;

    // Private constructor to prevent instantiation
    private COMQueryResponseDatasetHelper() {}

    /**
     * Converts a Spark DataFrame/Dataset<Row> to a COMQueryResponseBuilder.
     * 
     * <p>This method extracts the schema information from the dataset to create column definitions,
     * and extracts the row data to populate the rows in the response. It handles all Spark data types
     * and maps them to appropriate MySQL column types.</p>
     * 
     * <p>Note: This method calls {@code dataset.collectAsList()} which will collect all data from
     * the distributed dataset to the driver. Be cautious when using this with large datasets as it
     * may cause out of memory errors on the driver. Consider limiting the size of the dataset
     * before calling this method, for example by using {@code dataset.limit(1000)}.</p>
     *
     * @param dataset the Spark DataFrame/Dataset<Row> to convert
     * @return a configured COMQueryResponseBuilder ready to build MySQL protocol packets
     * @throws IllegalArgumentException if the dataset is null
     */
    public static COMQueryResponseBuilder fromDataset(final int capabilitiesFlag, final Dataset<Row> dataset) {
        var builder = COMQueryResponseBuilder.create(capabilitiesFlag);

        // Set column count
        var schema = dataset.schema();

        // Add column definitions
        for (final StructField field: schema.fields()) {
            builder.withColumnDefinition(createColumnDefinition(field));
        }

        // Add rows
        var rows = dataset.collectAsList();
        for (final Row row: rows) {
            builder.withRow(convertRowToStringList(row, schema));
        }

        return builder;
    }

    /**
     * Creates a column definition from a Spark StructField.
     *
     * @param field the Spark StructField
     * @return a ColumnDefinition for the MySQL protocol
     */
    public static COMQueryResponseBuilder.ColumnDefinition createColumnDefinition(final StructField field) {
        return new COMQueryResponseBuilder.ColumnDefinition(
                "",
                "",
                "",
                field.name(),
                field.name(),
                DEFAULT_CHARACTER_SET,
                getColumnLength(field.dataType()),
                getColumnType(field.dataType()),
                0,
                getDecimalPrecision(field.dataType())
        );
    }

    private static byte getColumnType(final DataType dataType) {
        return switch (dataType) {
            case StringType stringType -> MYSQL_TYPE_VARCHAR;
            case IntegerType integerType -> MYSQL_TYPE_LONG;
            case LongType longType -> MYSQL_TYPE_LONGLONG;
            case ShortType shortType -> MYSQL_TYPE_SHORT;
            case ByteType byteType -> MYSQL_TYPE_TINY;
            case FloatType floatType -> MYSQL_TYPE_FLOAT;
            case DoubleType doubleType -> MYSQL_TYPE_DOUBLE;
            case DecimalType decimalType -> MYSQL_TYPE_NEWDECIMAL;
            case BooleanType booleanType -> MYSQL_TYPE_TINY;
            case TimestampType timestampType -> MYSQL_TYPE_TIMESTAMP;
            case DateType dateType -> MYSQL_TYPE_DATE;
            case BinaryType binaryType -> MYSQL_TYPE_BLOB;
            default-> MYSQL_TYPE_VAR_STRING;
        };
    }

    private static int getColumnLength(final DataType dataType) {
        return switch (dataType) {
            case IntegerType integerType -> 4;
            case LongType longType -> 8;
            case ShortType shortType -> 2;
            case ByteType byteType -> 1;
            case FloatType floatType -> 4;
            case DoubleType doubleType -> 8;
            case DecimalType decimalType -> decimalType.precision();
            case BooleanType booleanType -> 1;
            case TimestampType timestampType -> 4;
            case DateType dateType -> 3;
            default-> 65535;
        };
    }

    /**
     * Gets the decimal precision for a data type.
     *
     * @param dataType the Spark data type
     * @return the decimal precision as a byte
     */
    private static byte getDecimalPrecision(final DataType dataType) {
        return switch (dataType) {
            case DecimalType decimalType -> (byte) decimalType.scale();
            case FloatType floatType -> 7;  // Default precision for FLOAT
            case DoubleType doubleType -> 15; // Default precision for DOUBLE
            case null, default -> 0;  // No decimal places for other types
        };
    }

    /**
     * Converts a Spark Row to a list of strings for the MySQL protocol.
     *
     * @param row the Spark Row
     * @param schema the schema of the Row
     * @return a list of string representations of the row values
     */
    public static List<String> convertRowToStringList(final Row row, final StructType schema) {
        var result = new ArrayList<String>();

        for (int i = 0; i < schema.fields().length; i++) {
            if (row.isNullAt(i)) {
                result.add(null);
            } else {
                // Convert the value to string based on its type
                var dataType = schema.fields()[i].dataType();
                result.add(convertValueToString(row.get(i), dataType));
            }
        }

        return result;
    }

    /**
     * Converts a value to its string representation based on its data type.
     *
     * @param value the value to convert
     * @param dataType the data type of the value
     * @return the string representation of the value
     */
    private static String convertValueToString(final Object value, final DataType dataType) {
        if (value == null) {
            return null;
        }

        // For simple types, just use toString()
        return value.toString();
    }

}
