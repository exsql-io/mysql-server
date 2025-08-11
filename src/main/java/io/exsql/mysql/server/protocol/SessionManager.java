package io.exsql.mysql.server.protocol;

import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import org.apache.spark.sql.classic.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.netty.Connection;

import javax.net.ssl.SSLException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class SessionManager {

    private final static Logger LOGGER = LoggerFactory.getLogger(SessionManager.class);

    private final static int MAX_SESSION_ID = Integer.MAX_VALUE;
    
    // Configuration constants
    private final static int DEFAULT_MAX_CONNECTIONS = 100;
    private final static Duration DEFAULT_SESSION_TIMEOUT = Duration.ofMinutes(30);
    private final static Duration CLEANUP_INTERVAL = Duration.ofMinutes(5);
    
    // Environment variable names for configuration
    private final static String ENV_MAX_CONNECTIONS = "MYSQL_SERVER_MAX_CONNECTIONS";
    private final static String ENV_SESSION_TIMEOUT_MINUTES = "MYSQL_SERVER_SESSION_TIMEOUT_MINUTES";

    private final SslContext sslContext;
    private final Map<Integer, SessionInfo> sessions;
    private final ReentrantLock lock = new ReentrantLock(true);
    private final SparkSession spark;
    private final ScheduledExecutorService cleanupExecutor;
    
    // Configuration
    private final int maxConnections;
    private final Duration sessionTimeout;
    
    // Metrics
    private final AtomicInteger connectionCount = new AtomicInteger(0);
    
    private int sessionId = 0;

    public SessionManager(final SparkSession spark) throws SSLException, CertificateException {
        var ssc = new SelfSignedCertificate();
        this.sslContext = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        this.sessions = new ConcurrentHashMap<>();
        this.spark = spark;
        
        // Load configuration from environment variables
        this.maxConnections = loadMaxConnections();
        this.sessionTimeout = loadSessionTimeout();
        
        // Start cleanup scheduler
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "session-cleanup");
            t.setDaemon(true);
            return t;
        });
        
        this.cleanupExecutor.scheduleWithFixedDelay(
            this::cleanupExpiredSessions,
            CLEANUP_INTERVAL.toMinutes(),
            CLEANUP_INTERVAL.toMinutes(),
            TimeUnit.MINUTES
        );
        
        LOGGER.info("SessionManager initialized: maxConnections={}, sessionTimeout={}", 
                   maxConnections, sessionTimeout);
    }

    public void initialize(final Connection connection) {
        // Check connection limit
        int currentConnections = connectionCount.get();
        if (currentConnections >= maxConnections) {
            LOGGER.warn("Connection limit reached ({}), rejecting new connection from {}", 
                       maxConnections, connection.channel().remoteAddress());
            connection.dispose();
            return;
        }
        
        var id = nextSessionId();

        LOGGER.debug("Initializing session[{}]: {}:{} (connections: {}/{})", 
                    id, connection.channel().remoteAddress(), connection.channel().localAddress(),
                    currentConnections + 1, maxConnections);

        var session = new Session(this, this.spark.newSession(), id, connection);
        var sessionInfo = new SessionInfo(session, Instant.now());
        
        sessions.put(id, sessionInfo);
        connectionCount.incrementAndGet();
        
        // Setup connection disposal handler to clean up when connection closes
        connection.onDispose(() -> {
            sessions.remove(id);
            connectionCount.decrementAndGet();
            LOGGER.debug("Session[{}] disposed (connections: {}/{})", 
                        id, connectionCount.get(), maxConnections);
        });
        
        session.initialize();
    }

    public void close(final int id) {
        var sessionInfo = sessions.remove(id);
        if (sessionInfo != null) {
            sessionInfo.session().close();
            connectionCount.decrementAndGet();
        }
    }

    public void shutdown() {
        LOGGER.info("Shutting down SessionManager with {} active sessions", sessions.size());
        
        // Close all sessions
        sessions.values().forEach(sessionInfo -> sessionInfo.session().close());
        sessions.clear();
        connectionCount.set(0);
        
        // Shutdown cleanup executor
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public SslContext sslContext() {
        return sslContext;
    }

    private int nextSessionId() {
        lock.lock();
        try {
            if (sessionId == MAX_SESSION_ID) {
                sessionId = 0;
                return sessionId;
            }

            return sessionId++;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Load maximum connections from environment variable or use default.
     */
    private int loadMaxConnections() {
        String value = System.getenv(ENV_MAX_CONNECTIONS);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid {} value '{}', using default {}", ENV_MAX_CONNECTIONS, value, DEFAULT_MAX_CONNECTIONS);
            }
        }
        return DEFAULT_MAX_CONNECTIONS;
    }
    
    /**
     * Load session timeout from environment variable or use default.
     */
    private Duration loadSessionTimeout() {
        String value = System.getenv(ENV_SESSION_TIMEOUT_MINUTES);
        if (value != null) {
            try {
                return Duration.ofMinutes(Long.parseLong(value));
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid {} value '{}', using default {}", ENV_SESSION_TIMEOUT_MINUTES, value, DEFAULT_SESSION_TIMEOUT);
            }
        }
        return DEFAULT_SESSION_TIMEOUT;
    }
    
    /**
     * Cleanup expired sessions periodically.
     */
    private void cleanupExpiredSessions() {
        try {
            Instant cutoff = Instant.now().minus(sessionTimeout);
            var expiredSessions = sessions.entrySet().stream()
                .filter(entry -> entry.getValue().createdAt().isBefore(cutoff))
                .map(Map.Entry::getKey)
                .toList();
                
            if (!expiredSessions.isEmpty()) {
                LOGGER.info("Cleaning up {} expired sessions", expiredSessions.size());
                expiredSessions.forEach(this::close);
            }
        } catch (Exception e) {
            LOGGER.error("Error during session cleanup", e);
        }
    }
    
    /**
     * Get current connection statistics.
     */
    public ConnectionStats getConnectionStats() {
        return new ConnectionStats(
            connectionCount.get(),
            maxConnections,
            sessions.size()
        );
    }
    
    /**
     * Information about a session including creation time for timeout management.
     */
    public record SessionInfo(Session session, Instant createdAt) {}
    
    /**
     * Connection statistics for monitoring.
     */
    public record ConnectionStats(int activeConnections, int maxConnections, int totalSessions) {}

}
