package io.exsql.mysql.server.util;

import io.exsql.mysql.server.MysqlServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Base class for integration tests that need a running MySQL server.
 * Extends SparkTestBase to provide both Spark session and MySQL server.
 */
public abstract class MySQLServerTestBase extends SparkTestBase {

    protected static MysqlServer mysqlServer;
    protected static int serverPort;
    protected static String serverHost = "127.0.0.1";
    
    private static final int DEFAULT_PORT = 0; // Use random port
    private static final Duration SERVER_START_TIMEOUT = Duration.ofSeconds(30);

    @BeforeAll
    static void setupMySQLServer() throws Exception {
        // Ensure Spark is set up first
        SparkTestBase.setupSpark();
        
        // Create MySQL server with test configuration
        mysqlServer = MysqlServer.create(serverHost, DEFAULT_PORT, spark);
        
        // Start server in background thread
        CountDownLatch serverStarted = new CountDownLatch(1);
        Thread serverThread = new Thread(() -> {
            try {
                mysqlServer.start(); // This blocks until server stops
            } catch (Exception e) {
                System.err.println("Failed to start test MySQL server: " + e.getMessage());
                e.printStackTrace();
            }
        });
        
        serverThread.setDaemon(true);
        serverThread.start();
        
        // Wait for server to initialize and get the actual port
        long startTime = System.currentTimeMillis();
        while (mysqlServer.port() == 0 && (System.currentTimeMillis() - startTime) < SERVER_START_TIMEOUT.toMillis()) {
            Thread.sleep(100);
        }
        
        if (mysqlServer.port() == 0) {
            throw new RuntimeException("MySQL server failed to start within " + SERVER_START_TIMEOUT);
        }
        
        serverPort = mysqlServer.port();
        serverHost = mysqlServer.host();
        System.out.println("Test MySQL server started on " + serverHost + ":" + serverPort);
        
        // Give server a moment to fully initialize
        Thread.sleep(1000);
    }

    @AfterAll
    static void tearDownMySQLServer() throws Exception {
        if (mysqlServer != null) {
            System.out.println("Stopping test MySQL server...");
            mysqlServer.stop();
            mysqlServer = null;
        }
        
        // Clean up Spark
        SparkTestBase.tearDownSpark();
    }
    
    /**
     * Get the MySQL server port for connecting.
     */
    protected static int getServerPort() {
        return serverPort;
    }
    
    /**
     * Get the MySQL server host for connecting.
     */
    protected static String getServerHost() {
        return serverHost;
    }
    
    /**
     * Get the JDBC URL for connecting to the test MySQL server.
     */
    protected static String getJdbcUrl() {
        return String.format("jdbc:mysql://%s:%d", serverHost, serverPort);
    }
    
    /**
     * Get the JDBC URL for connecting to a specific database.
     */
    protected static String getJdbcUrl(String database) {
        return String.format("jdbc:mysql://%s:%d/%s", serverHost, serverPort, database);
    }
}