package io.exsql.mysql.server.performance;

import io.exsql.mysql.server.util.MySQLClientTestBase;
import io.exsql.mysql.server.util.TestDataGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import java.sql.*;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance tests for large result sets.
 * Tests the server's streaming capabilities and memory management.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class LargeResultSetTest extends MySQLClientTestBase {

    @BeforeAll
    static void setupTestData() {
        // Create base test data
        TestDataGenerator.registerAllTestTables(spark);
        
        // Create larger datasets for performance testing
        createLargeTestData();
    }

    private static void createLargeTestData() {
        // Create a large table with 10,000 rows
        TestDataGenerator.createLargeTable(spark, 10000)
            .createOrReplaceTempView("large_test_table");
        
        // Create a table with wide rows (many columns)
        spark.sql("""
            CREATE OR REPLACE TEMPORARY VIEW wide_test_table AS
            SELECT 
                id,
                CONCAT('Name_', id) as name1,
                CONCAT('Description_', id) as desc1,
                CONCAT('Value_', id) as value1,
                CONCAT('Extra_', id) as extra1,
                CONCAT('Field_', id) as field1,
                CONCAT('Data_', id) as data1,
                CONCAT('Info_', id) as info1,
                CONCAT('Text_', id) as text1,
                CONCAT('Content_', id) as content1
            FROM (SELECT id FROM default.default_persons LIMIT 1000)
        """);
    }

    @Test
    @DisplayName("01. Test querying large result set (10K rows)")
    void testLargeResultSet() throws SQLException {
        long startTime = System.currentTimeMillis();
        
        List<List<Object>> results = executeQuery("SELECT * FROM large_test_table");
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify we got all the data
        assertThat(results).hasSize(10000);
        
        // Should complete within reasonable time (less than 30 seconds)
        assertThat(duration).isLessThan(30000);
        
        System.out.println("Large result set query completed in " + duration + "ms");
    }

    @Test
    @DisplayName("02. Test streaming large result set with JDBC")
    void testStreamingLargeResultSet() throws SQLException {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            
            // Enable streaming (fetch size)
            stmt.setFetchSize(100);
            
            long startTime = System.currentTimeMillis();
            
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM large_test_table ORDER BY id")) {
                int rowCount = 0;
                long firstRowTime = 0;
                
                while (rs.next()) {
                    rowCount++;
                    
                    // Record time to first row
                    if (rowCount == 1) {
                        firstRowTime = System.currentTimeMillis();
                    }
                    
                    // Verify data integrity for some rows
                    if (rowCount % 1000 == 0) {
                        long id = rs.getLong("id");
                        String data1 = rs.getString("data1");
                        assertThat(id).isPositive();
                        assertThat(data1).startsWith("Data row " + id);
                    }
                }
                
                long endTime = System.currentTimeMillis();
                long totalDuration = endTime - startTime;
                long timeToFirstRow = firstRowTime - startTime;
                
                // Verify we got all rows
                assertThat(rowCount).isEqualTo(10000);
                
                // Time to first row should be quick (streaming)
                assertThat(timeToFirstRow).isLessThan(5000); // Less than 5 seconds to first row
                
                // Total time should be reasonable
                assertThat(totalDuration).isLessThan(60000); // Less than 1 minute total
                
                System.out.println("Streaming query: " + timeToFirstRow + "ms to first row, " + 
                                 totalDuration + "ms total for " + rowCount + " rows");
            }
        }
    }

    @Test
    @DisplayName("03. Test wide result set (many columns)")
    void testWideResultSet() throws SQLException {
        long startTime = System.currentTimeMillis();
        
        List<List<Object>> results = executeQuery("SELECT * FROM wide_test_table");
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify structure
        assertThat(results).isNotEmpty();
        assertThat(results.get(0)).hasSize(10); // 10 columns
        
        // Should complete within reasonable time
        assertThat(duration).isLessThan(15000); // Less than 15 seconds
        
        System.out.println("Wide result set query completed in " + duration + "ms");
    }

    @Test
    @DisplayName("04. Test COUNT query on large table")
    void testCountQueryOnLargeTable() throws SQLException {
        long startTime = System.currentTimeMillis();
        
        List<List<Object>> results = executeQuery("SELECT COUNT(*) FROM large_test_table");
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify count
        assertThat(results).hasSize(1);
        Long count = (Long) results.get(0).get(0);
        assertThat(count).isEqualTo(10000L);
        
        // COUNT should be relatively fast
        assertThat(duration).isLessThan(10000); // Less than 10 seconds
        
        System.out.println("COUNT query completed in " + duration + "ms");
    }

    @Test
    @DisplayName("05. Test aggregation query on large table")
    void testAggregationQueryOnLargeTable() throws SQLException {
        long startTime = System.currentTimeMillis();
        
        List<List<Object>> results = executeQuery("""
            SELECT 
                COUNT(*) as total_rows,
                MIN(id) as min_id,
                MAX(id) as max_id,
                AVG(data2) as avg_data2
            FROM large_test_table
        """);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify results
        assertThat(results).hasSize(1);
        List<Object> row = results.get(0);
        
        Long totalRows = (Long) row.get(0);
        Long minId = (Long) row.get(1);
        Long maxId = (Long) row.get(2);
        Double avgData2 = (Double) row.get(3);
        
        assertThat(totalRows).isEqualTo(10000L);
        assertThat(minId).isEqualTo(1L);
        assertThat(maxId).isEqualTo(10000L);
        assertThat(avgData2).isPositive();
        
        // Aggregation should complete within reasonable time
        assertThat(duration).isLessThan(15000); // Less than 15 seconds
        
        System.out.println("Aggregation query completed in " + duration + "ms");
    }

    @Test
    @DisplayName("06. Test LIMIT query performance")
    void testLimitQueryPerformance() throws SQLException {
        // Test different LIMIT sizes
        int[] limitSizes = {10, 100, 1000, 5000};
        
        for (int limit : limitSizes) {
            long startTime = System.currentTimeMillis();
            
            List<List<Object>> results = executeQuery(
                "SELECT * FROM large_test_table ORDER BY id LIMIT " + limit);
            
            long endTime = System.currentTimeMillis();
            long duration = endTime - startTime;
            
            // Verify we got the right number of rows
            assertThat(results).hasSize(limit);
            
            // Should be relatively fast, especially for smaller limits
            if (limit <= 100) {
                assertThat(duration).isLessThan(5000); // Less than 5 seconds for small limits
            } else {
                assertThat(duration).isLessThan(15000); // Less than 15 seconds for larger limits
            }
            
            System.out.println("LIMIT " + limit + " query completed in " + duration + "ms");
        }
    }

    @Test
    @DisplayName("07. Test ORDER BY performance on large table")
    void testOrderByPerformance() throws SQLException {
        long startTime = System.currentTimeMillis();
        
        // Order by a column and limit to avoid returning too much data
        List<List<Object>> results = executeQuery(
            "SELECT id, data1 FROM large_test_table ORDER BY data2 DESC LIMIT 100");
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify we got results
        assertThat(results).hasSize(100);
        
        // ORDER BY might take longer, but should still be reasonable
        assertThat(duration).isLessThan(30000); // Less than 30 seconds
        
        System.out.println("ORDER BY query completed in " + duration + "ms");
    }

    @Test
    @DisplayName("08. Test JOIN performance")
    void testJoinPerformance() throws SQLException {
        long startTime = System.currentTimeMillis();
        
        // Join large table with smaller table
        List<List<Object>> results = executeQuery("""
            SELECT l.id, l.data1, p.name 
            FROM large_test_table l 
            JOIN default.default_persons p ON l.id = p.id 
            LIMIT 100
        """);
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Should get some results (limited by the smaller table)
        assertThat(results).isNotEmpty();
        assertThat(results.size()).isLessThanOrEqualTo(100);
        
        // JOIN should complete within reasonable time
        assertThat(duration).isLessThan(20000); // Less than 20 seconds
        
        System.out.println("JOIN query completed in " + duration + "ms, returned " + results.size() + " rows");
    }

    @Test
    @DisplayName("09. Test memory usage with large result sets")
    void testMemoryUsageWithLargeResultSets() throws SQLException {
        // Get memory before query
        Runtime runtime = Runtime.getRuntime();
        runtime.gc(); // Suggest garbage collection
        long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
        
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            
            // Use streaming to avoid loading everything into memory
            stmt.setFetchSize(100);
            
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM large_test_table")) {
                int rowCount = 0;
                long maxMemoryUsed = 0;
                
                while (rs.next()) {
                    rowCount++;
                    
                    // Check memory usage periodically
                    if (rowCount % 1000 == 0) {
                        long currentMemory = runtime.totalMemory() - runtime.freeMemory();
                        long memoryUsed = currentMemory - memoryBefore;
                        maxMemoryUsed = Math.max(maxMemoryUsed, memoryUsed);
                        
                        // Memory usage should not grow excessively with streaming
                        assertThat(memoryUsed).isLessThan(500 * 1024 * 1024); // Less than 500MB
                    }
                }
                
                assertThat(rowCount).isEqualTo(10000);
                System.out.println("Max memory used during streaming: " + (maxMemoryUsed / 1024 / 1024) + "MB");
            }
        }
    }

    @Test
    @DisplayName("10. Test concurrent large queries")
    void testConcurrentLargeQueries() throws Exception {
        // Test multiple large queries running simultaneously
        int numberOfThreads = 3;
        Thread[] threads = new Thread[numberOfThreads];
        boolean[] completed = new boolean[numberOfThreads];
        Exception[] exceptions = new Exception[numberOfThreads];
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < numberOfThreads; i++) {
            final int threadIndex = i;
            
            threads[i] = new Thread(() -> {
                try {
                    List<List<Object>> results = executeQuery(
                        "SELECT COUNT(*) FROM large_test_table WHERE data2 > " + (threadIndex * 100000));
                    
                    assertThat(results).hasSize(1);
                    completed[threadIndex] = true;
                    
                } catch (Exception e) {
                    exceptions[threadIndex] = e;
                }
            });
            
            threads[i].start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join(30000); // 30 second timeout
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        // Verify all queries completed successfully
        for (int i = 0; i < numberOfThreads; i++) {
            if (exceptions[i] != null) {
                throw new RuntimeException("Thread " + i + " failed", exceptions[i]);
            }
            assertThat(completed[i]).isTrue();
        }
        
        // Should complete within reasonable time
        assertThat(duration).isLessThan(60000); // Less than 1 minute
        
        System.out.println("Concurrent large queries completed in " + duration + "ms");
    }
}