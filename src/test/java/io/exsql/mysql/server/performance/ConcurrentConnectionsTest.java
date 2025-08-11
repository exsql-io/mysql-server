package io.exsql.mysql.server.performance;

import io.exsql.mysql.server.util.MySQLClientTestBase;
import io.exsql.mysql.server.util.TestDataGenerator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Performance tests for concurrent connections.
 * Tests the server's ability to handle multiple simultaneous clients.
 */
@TestMethodOrder(MethodOrderer.DisplayName.class)
class ConcurrentConnectionsTest extends MySQLClientTestBase {

    private static final int SMALL_CONCURRENT_CONNECTIONS = 5;
    private static final int MEDIUM_CONCURRENT_CONNECTIONS = 10;
    private static final int LARGE_CONCURRENT_CONNECTIONS = 20;
    private static final int TIMEOUT_SECONDS = 30;

    @BeforeAll
    static void setupTestData() {
        TestDataGenerator.registerAllTestTables(spark);
    }

    @Test
    @DisplayName("01. Test small number of concurrent connections")
    void testSmallConcurrentConnections() throws Exception {
        testConcurrentConnections(SMALL_CONCURRENT_CONNECTIONS);
    }

    @Test
    @DisplayName("02. Test medium number of concurrent connections")
    void testMediumConcurrentConnections() throws Exception {
        testConcurrentConnections(MEDIUM_CONCURRENT_CONNECTIONS);
    }

    @Test
    @DisplayName("03. Test large number of concurrent connections")
    void testLargeConcurrentConnections() throws Exception {
        testConcurrentConnections(LARGE_CONCURRENT_CONNECTIONS);
    }

    @Test
    @DisplayName("04. Test concurrent queries on same table")
    void testConcurrentQueriesOnSameTable() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(MEDIUM_CONCURRENT_CONNECTIONS);
        List<Future<List<List<Object>>>> futures = new ArrayList<>();

        try {
            // Submit multiple queries to the same table
            for (int i = 0; i < MEDIUM_CONCURRENT_CONNECTIONS; i++) {
                Future<List<List<Object>>> future = executor.submit(() -> {
                    try {
                        return executeQuery("SELECT COUNT(*) FROM default.default_persons");
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }

            // Collect results
            List<List<List<Object>>> results = new ArrayList<>();
            for (Future<List<List<Object>>> future : futures) {
                results.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            // All queries should return the same count
            assertThat(results).hasSize(MEDIUM_CONCURRENT_CONNECTIONS);
            
            Long expectedCount = (Long) results.get(0).get(0).get(0);
            for (List<List<Object>> result : results) {
                assertThat(result).hasSize(1);
                assertThat(result.get(0).get(0)).isEqualTo(expectedCount);
            }

        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("05. Test concurrent database switching")
    void testConcurrentDatabaseSwitching() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(MEDIUM_CONCURRENT_CONNECTIONS);
        List<Future<String>> futures = new ArrayList<>();

        try {
            // Each thread switches to a different database
            for (int i = 0; i < MEDIUM_CONCURRENT_CONNECTIONS; i++) {
                final String database = (i % 2 == 0) ? "default" : "information_schema";
                
                Future<String> future = executor.submit(() -> {
                    try (Connection conn = createConnection()) {
                        executeUpdate(conn, "USE " + database);
                        
                        // Verify we're in the right database by checking table count
                        List<List<Object>> tables = executeQuery(conn, "SHOW TABLES");
                        
                        if ("default".equals(database)) {
                            // Should have at least 2 tables (default_persons, default_orders)
                            assertThat(tables.size()).isGreaterThanOrEqualTo(2);
                        } else {
                            // Should have exactly 3 tables (SCHEMATA, TABLES, COLUMNS)
                            assertThat(tables).hasSize(3);
                        }
                        
                        return database;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }

            // Collect results
            List<String> results = new ArrayList<>();
            for (Future<String> future : futures) {
                results.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertThat(results).hasSize(MEDIUM_CONCURRENT_CONNECTIONS);
            assertThat(results).contains("default", "information_schema");

        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("06. Test concurrent SHOW commands")
    void testConcurrentShowCommands() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(MEDIUM_CONCURRENT_CONNECTIONS);
        List<Future<Integer>> futures = new ArrayList<>();

        String[] showCommands = {
            "SHOW DATABASES",
            "SHOW TABLES",
            "SHOW COLUMNS FROM default.default_persons"
        };

        try {
            // Submit different SHOW commands concurrently
            for (int i = 0; i < MEDIUM_CONCURRENT_CONNECTIONS; i++) {
                final String command = showCommands[i % showCommands.length];
                
                Future<Integer> future = executor.submit(() -> {
                    try {
                        List<List<Object>> results = executeQuery(command);
                        return results.size();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }

            // Collect results
            List<Integer> results = new ArrayList<>();
            for (Future<Integer> future : futures) {
                results.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertThat(results).hasSize(MEDIUM_CONCURRENT_CONNECTIONS);
            
            // All results should be positive (non-empty)
            for (Integer count : results) {
                assertThat(count).isPositive();
            }

        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("07. Test connection pool exhaustion handling")
    void testConnectionPoolExhaustion() throws Exception {
        // This test tries to create many connections to test the server's limits
        List<Connection> connections = new ArrayList<>();
        
        try {
            // Try to create many connections
            for (int i = 0; i < 100; i++) { // Try to create more than the default limit
                try {
                    Connection conn = createConnection();
                    connections.add(conn);
                    
                    // Do a simple query to verify the connection works
                    executeQuery(conn, "SELECT 1");
                    
                } catch (SQLException e) {
                    // Expected when we hit the connection limit
                    System.out.println("Hit connection limit at connection " + i + ": " + e.getMessage());
                    break;
                }
            }
            
            // Should have created at least some connections
            assertThat(connections.size()).isGreaterThan(10);
            
        } finally {
            // Clean up all connections
            for (Connection conn : connections) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // Ignore cleanup errors
                }
            }
        }
    }

    @Test
    @DisplayName("08. Test concurrent information_schema queries")
    void testConcurrentInformationSchemaQueries() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(MEDIUM_CONCURRENT_CONNECTIONS);
        List<Future<Integer>> futures = new ArrayList<>();

        String[] infoQueries = {
            "SELECT COUNT(*) FROM information_schema.SCHEMATA",
            "SELECT COUNT(*) FROM information_schema.TABLES",
            "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'default'"
        };

        try {
            // Submit information_schema queries concurrently
            for (int i = 0; i < MEDIUM_CONCURRENT_CONNECTIONS; i++) {
                final String query = infoQueries[i % infoQueries.length];
                
                Future<Integer> future = executor.submit(() -> {
                    try {
                        List<List<Object>> results = executeQuery(query);
                        return ((Long) results.get(0).get(0)).intValue();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }

            // Collect results
            List<Integer> results = new ArrayList<>();
            for (Future<Integer> future : futures) {
                results.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertThat(results).hasSize(MEDIUM_CONCURRENT_CONNECTIONS);
            
            // All results should be positive
            for (Integer count : results) {
                assertThat(count).isPositive();
            }

        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    @DisplayName("09. Test performance under concurrent load")
    void testPerformanceUnderConcurrentLoad() throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(MEDIUM_CONCURRENT_CONNECTIONS);
        List<Future<Long>> futures = new ArrayList<>();

        try {
            long startTime = System.currentTimeMillis();

            // Submit multiple queries that should execute reasonably quickly
            for (int i = 0; i < MEDIUM_CONCURRENT_CONNECTIONS; i++) {
                Future<Long> future = executor.submit(() -> {
                    try {
                        long queryStart = System.currentTimeMillis();
                        executeQuery("SELECT COUNT(*) FROM default.default_persons WHERE age > 30");
                        long queryEnd = System.currentTimeMillis();
                        return queryEnd - queryStart;
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }

            // Collect timing results
            List<Long> queryTimes = new ArrayList<>();
            for (Future<Long> future : futures) {
                queryTimes.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            long totalTime = System.currentTimeMillis() - startTime;

            // Performance assertions
            assertThat(queryTimes).hasSize(MEDIUM_CONCURRENT_CONNECTIONS);
            
            // No individual query should take more than 10 seconds
            for (Long queryTime : queryTimes) {
                assertThat(queryTime).isLessThan(10000L);
            }
            
            // Total time should be reasonable (less than 30 seconds)
            assertThat(totalTime).isLessThan(30000L);
            
            // Average query time should be reasonable
            double avgQueryTime = queryTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
            assertThat(avgQueryTime).isLessThan(5000.0); // Less than 5 seconds average

        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Helper method to test basic concurrent connections.
     */
    private void testConcurrentConnections(int connectionCount) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(connectionCount);
        List<Future<Integer>> futures = new ArrayList<>();

        try {
            // Submit connection tasks
            for (int i = 0; i < connectionCount; i++) {
                final int connectionId = i;
                
                Future<Integer> future = executor.submit(() -> {
                    try (Connection conn = createConnection()) {
                        // Verify connection works
                        List<List<Object>> result = executeQuery(conn, "SELECT " + connectionId + " as connection_id");
                        return (Integer) result.get(0).get(0);
                    } catch (SQLException e) {
                        throw new RuntimeException("Connection " + connectionId + " failed", e);
                    }
                });
                futures.add(future);
            }

            // Collect results
            List<Integer> connectionIds = new ArrayList<>();
            for (Future<Integer> future : futures) {
                connectionIds.add(future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            // Verify all connections worked
            assertThat(connectionIds).hasSize(connectionCount);
            
            // Verify we got the expected connection IDs
            List<Integer> expectedIds = IntStream.range(0, connectionCount).boxed().toList();
            assertThat(connectionIds).containsExactlyInAnyOrderElementsOf(expectedIds);

        } finally {
            executor.shutdown();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }
}