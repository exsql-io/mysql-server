package io.exsql.mysql.server.util;

import org.apache.spark.sql.classic.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Base class for tests that need a Spark session.
 * Sets up a local Spark session for testing purposes.
 */
public abstract class SparkTestBase {

    protected static SparkSession spark;
    private static Path warehouseDir;

    @BeforeAll
    static void setupSpark() throws Exception {
        // Create temporary warehouse directory
        warehouseDir = Files.createTempDirectory("spark-warehouse-test");
        
        // Configure Spark for testing
        spark = SparkSession.builder()
                .appName("mysql-server-test")
                .master("local[*]")
                .config("spark.sql.warehouse.dir", warehouseDir.toString())
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.adaptive.enabled", "false") // Disable AQE for predictable tests
                .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
                .config("spark.ui.enabled", "false") // Disable Spark UI for tests
                .config("spark.sql.catalogImplementation", "in-memory") // Use in-memory catalog for tests
                .getOrCreate();
                
        // Set log level to reduce noise in tests
        spark.sparkContext().setLogLevel("WARN");
    }

    @AfterAll
    static void tearDownSpark() throws Exception {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
        
        // Clean up warehouse directory
        if (warehouseDir != null && Files.exists(warehouseDir)) {
            deleteDirectory(warehouseDir.toFile());
        }
    }
    
    /**
     * Recursively delete a directory and all its contents.
     */
    private static void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
    }
    
    /**
     * Get the Spark session for testing.
     */
    protected static SparkSession getSparkSession() {
        return spark;
    }
}