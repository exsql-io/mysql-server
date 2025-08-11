package io.exsql.mysql.server.util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Base class for tests that need MySQL client connectivity.
 * Provides utilities for connecting to and querying the MySQL server.
 */
public abstract class MySQLClientTestBase extends MySQLServerTestBase {

    // Default connection properties for tests
    private static final String DEFAULT_USER = "test";
    private static final String DEFAULT_PASSWORD = "test";
    private static final int DEFAULT_CONNECT_TIMEOUT = 10000; // 10 seconds
    private static final int DEFAULT_SOCKET_TIMEOUT = 30000; // 30 seconds

    /**
     * Create a MySQL connection with default properties.
     */
    protected Connection createConnection() throws SQLException {
        return createConnection(null, null);
    }

    /**
     * Create a MySQL connection to a specific database.
     */
    protected Connection createConnection(String database) throws SQLException {
        return createConnection(database, null);
    }

    /**
     * Create a MySQL connection with custom properties.
     */
    protected Connection createConnection(String database, Properties additionalProps) throws SQLException {
        String url = database != null ? getJdbcUrl(database) : getJdbcUrl();
        
        Properties props = new Properties();
        props.setProperty("user", DEFAULT_USER);
        props.setProperty("password", DEFAULT_PASSWORD);
        props.setProperty("connectTimeout", String.valueOf(DEFAULT_CONNECT_TIMEOUT));
        props.setProperty("socketTimeout", String.valueOf(DEFAULT_SOCKET_TIMEOUT));
        props.setProperty("autoReconnect", "false");
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        
        // Add any additional properties
        if (additionalProps != null) {
            props.putAll(additionalProps);
        }
        
        return DriverManager.getConnection(url, props);
    }

    /**
     * Execute a query and verify the result with a consumer.
     */
    protected void executeAndVerify(String sql, Consumer<ResultSet> verifier) throws SQLException {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            verifier.accept(rs);
        }
    }

    /**
     * Execute a query and return the result set as a list of rows.
     * Each row is represented as a list of column values.
     */
    protected List<List<Object>> executeQuery(String sql) throws SQLException {
        List<List<Object>> results = new ArrayList<>();
        
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getObject(i));
                }
                results.add(row);
            }
        }
        
        return results;
    }

    /**
     * Execute a query and return the result set as a list of rows.
     * Uses a specific connection.
     */
    protected List<List<Object>> executeQuery(Connection conn, String sql) throws SQLException {
        List<List<Object>> results = new ArrayList<>();
        
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            
            while (rs.next()) {
                List<Object> row = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.add(rs.getObject(i));
                }
                results.add(row);
            }
        }
        
        return results;
    }

    /**
     * Execute an update statement and return the affected row count.
     */
    protected int executeUpdate(String sql) throws SQLException {
        try (Connection conn = createConnection();
             Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    /**
     * Execute an update statement using a specific connection.
     */
    protected int executeUpdate(Connection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    /**
     * Verify that a query returns the expected number of rows.
     */
    protected void assertRowCount(String sql, int expectedCount) throws SQLException {
        List<List<Object>> results = executeQuery(sql);
        assertThat(results).hasSize(expectedCount);
    }

    /**
     * Verify that a query returns exactly one row with the expected values.
     */
    protected void assertSingleRow(String sql, Object... expectedValues) throws SQLException {
        List<List<Object>> results = executeQuery(sql);
        assertThat(results).hasSize(1);
        
        List<Object> row = results.get(0);
        assertThat(row).containsExactly(expectedValues);
    }

    /**
     * Verify that a query returns no rows.
     */
    protected void assertEmptyResult(String sql) throws SQLException {
        assertRowCount(sql, 0);
    }

    /**
     * Test that a connection can be established.
     */
    protected void assertCanConnect() throws SQLException {
        try (Connection conn = createConnection()) {
            assertThat(conn.isValid(5)).isTrue();
        }
    }

    /**
     * Test that a connection can be established to a specific database.
     */
    protected void assertCanConnect(String database) throws SQLException {
        try (Connection conn = createConnection(database)) {
            assertThat(conn.isValid(5)).isTrue();
            assertThat(conn.getCatalog()).isEqualTo(database);
        }
    }
}