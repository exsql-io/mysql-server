package io.exsql.mysql.server.protocol;

import com.google.common.collect.Lists;
import io.exsql.mysql.server.protocol.client.ComPacket;
import io.exsql.mysql.server.protocol.client.HandshakeResponse41Packet;
import io.exsql.mysql.server.protocol.server.ComQueryResponseDatasetHelper;
import io.exsql.mysql.server.protocol.server.ErrPacketBuilder;
import io.exsql.mysql.server.protocol.server.HandshakeV10Builder;
import io.exsql.mysql.server.protocol.server.OkPacketBuilder;
import io.exsql.mysql.server.protocol.server.StreamingTextResultsetPacketBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.classic.Dataset;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.netty.Connection;
import java.time.Duration;

import java.util.function.Function;

public class Session {

    private static final Logger LOGGER = LoggerFactory.getLogger(Session.class);

    // Query timeout configuration
    private static final Duration DEFAULT_QUERY_TIMEOUT = Duration.ofMinutes(10);
    private static final String ENV_QUERY_TIMEOUT_MINUTES = "MYSQL_SERVER_QUERY_TIMEOUT_MINUTES";

    private static final Row[] VERSION_COMMENT_ROW = new GenericRow[] {
            new GenericRow(new String[] { "data-platform-mysql-frontend" })
    };

    private static final StructType VERSION_COMMENT_SCHEMA = StructType.fromDDL("version_comment string");

    private static final Encoder<Row> VERSION_COMMENT_ENCODER = Encoders.row(VERSION_COMMENT_SCHEMA);

    private final SessionManager manager;
    private final SparkSession spark;
    private final int id;
    private final Duration queryTimeout;
    private final InformationSchemaHandler informationSchemaHandler;
    
    private Connection connection;
    private SessionPhase phase;
    private short sequenceId;
    private HandshakeResponse41Packet handshakeResponse;
    private String currentDatabase;

    public Session(final SessionManager manager, final SparkSession spark, final int id, final Connection connection) {
        this.manager = manager;
        this.spark = spark;
        this.id = id;
        this.connection = connection;
        this.phase = SessionPhase.CONNECTION_PHASE_INITIAL_HANDSHAKE;
        this.queryTimeout = loadQueryTimeout();
        this.currentDatabase = "default"; // Default database
        this.informationSchemaHandler = new InformationSchemaHandler(spark);
    }

    public void initialize() {
        this.send(byteBufAllocator -> {
            var buffer = byteBufAllocator.buffer();
            HandshakeV10Builder.create(this.id).withSequenceId((byte) sequenceId).build(buffer);
            return buffer;
        });

        this.waitForResponse();
    }

    public void close() {
        LOGGER.debug("Closing session[{}]: {}:{}", id, connection.channel().remoteAddress(), connection.channel().localAddress());
        connection.disposeNow();
    }

    private void send(final Function<ByteBufAllocator, ByteBuf> producer) {
        var buffer = producer.apply(connection.outbound().alloc());
        connection
                .outbound()
                .send(Mono.just(buffer))
                .then()
                .subscribe()
                .dispose();

        incrementSequenceId();
    }

    private void signal() {
        connection
                .outbound()
                .then()
                .subscribe()
                .dispose();
    }

    private void waitForResponse() {
        connection
                .inbound()
                .receive()
                .doOnEach(signal -> {
                    var inbound = signal.get();
                    incrementSequenceId();

                    switch (phase) {
                        case CONNECTION_PHASE_INITIAL_HANDSHAKE:
                            // Consume it for now, will need to validate the user later.
                            this.handshakeResponse = HandshakeResponse41Packet.parse(inbound);
                            if (handshakeResponse.isSSLRequestPacket()) {
                                useSSL();
                                signal();
                            } else {
                                send(byteBufAllocator -> {
                                    var buffer = byteBufAllocator.buffer();
                                    OkPacketBuilder
                                            .create()
                                            .withHeader(OkPacketBuilder.OK_PACKET_HEADER)
                                            .withStatusFlags(0)
                                            .withSequenceId((byte) sequenceId)
                                            .build(buffer);

                                    return buffer;
                                });
                                phase = SessionPhase.COMMAND_PHASE;
                            }

                            break;
                        case COMMAND_PHASE:
                            var packet = ComPacket.parse(inbound);
                            switch (packet.header().kind()) {
                                case QUERY -> {
                                    var query = packet.getBody(String.class);
                                    resetSequenceId(packet.header().sequenceId());
                                    executeQueryAsync(query);
                                }
                                case INIT_DB -> {
                                    var databaseName = packet.getBody(String.class);
                                    resetSequenceId(packet.header().sequenceId());
                                    handleInitDatabase(databaseName);
                                }
                                case QUIT -> this.manager.close(this.id);
                                case UNKNOWN ->
                                        send(byteBufAllocator -> {
                                            var buffer = byteBufAllocator.buffer();
                                            ErrPacketBuilder
                                                    .create()
                                                    .withErrorCode((short) 1105)
                                                    .withSequenceId((byte) sequenceId)
                                                    .build(buffer);
                                            return buffer;
                                        });
                            }

                            break;
                    }
                })
                .then().subscribe(connection.disposeSubscriber());
    }

    private void incrementSequenceId() {
        sequenceId++;
        if (sequenceId > 0xFF) {
            this.sequenceId = 0;
        }
    }

    private void resetSequenceId(final byte to) {
        this.sequenceId = (byte) (to + 1);
    }

    private void useSSL() {
        this.connection = this.connection.addHandlerFirst("ssl", this.manager.sslContext().newHandler(this.connection.channel().alloc()));
    }

    /**
     * Execute a SQL query asynchronously to avoid blocking the Netty event loop.
     * This method moves Spark operations to a separate thread pool and handles
     * result streaming back to the client asynchronously.
     */
    private void executeQueryAsync(final String query) {
        Mono.fromCallable(() -> {
            // Execute Spark operations on a separate thread pool
            try {
                Dataset<Row> ds = null;
                
                // Handle special queries before passing to Spark
                if (query.equalsIgnoreCase("SELECT @@version_comment LIMIT 1")) {
                    ds = spark.createDataset(Lists.newArrayList(VERSION_COMMENT_ROW), VERSION_COMMENT_ENCODER);
                } else if (isUseStatement(query)) {
                    // Handle USE database statements
                    return handleUseStatement(query);
                } else if ("information_schema".equalsIgnoreCase(currentDatabase) || 
                          InformationSchemaHandler.isInformationSchemaQuery(query)) {
                    // Handle information_schema queries
                    ds = informationSchemaHandler.processQuery(query, currentDatabase);
                } else {
                    ds = spark.sql(query);
                }
                return ComQueryResponseDatasetHelper.fromDataset(this.handshakeResponse.clientFlag(), ds);
            } catch (Exception e) {
                throw new RuntimeException("Query execution failed: " + query, e);
            }
        })
        .subscribeOn(Schedulers.boundedElastic()) // Use bounded elastic scheduler for blocking operations
        .timeout(queryTimeout) // Add query timeout
        .subscribe(
            stream -> {
                // Success: stream results back to client
                try {
                    while (stream.hasNext()) {
                        send(byteBufAllocator -> {
                            var buffer = byteBufAllocator.buffer();
                            try {
                                stream.next().withSequenceId((byte) sequenceId).build(buffer);
                                return buffer;
                            } catch (IllegalStateException e) {
                                // Handle packet size limit exceeded
                                if (e.getMessage().contains("exceeds MySQL limit")) {
                                    LOGGER.warn("Session[{}]: Packet size limit exceeded for query: {}", id, query);
                                    throw new RuntimeException("Result set too large for MySQL protocol", e);
                                }
                                throw e;
                            }
                        });
                    }
                } catch (Exception e) {
                    LOGGER.error("Session[{}]: Error streaming results for query: {}", id, query, e);
                    if (e.getMessage().contains("too large") || e.getCause() instanceof IllegalStateException) {
                        sendErrorResponse(1105, "Result set too large - consider adding LIMIT clause or reducing column sizes");
                    } else {
                        sendErrorResponse(1105, "Error streaming results");
                    }
                }
            },
            throwable -> {
                // Error: send error response to client
                LOGGER.debug("Session[{}]: Query execution failed for: {}. Error: {}", id, query, throwable.getMessage(), throwable);
                
                if (throwable instanceof java.util.concurrent.TimeoutException) {
                    LOGGER.warn("Session[{}]: Query timed out after {} for: {}", id, queryTimeout, query);
                    sendErrorResponse(1969, "Query execution timed out");
                } else if (throwable.getCause() instanceof AnalysisException ae) {
                    sendErrorResponse(1064, "42000", ae.getSimpleMessage());
                } else {
                    sendErrorResponse(1105, "Query execution failed");
                }
            }
        );
    }

    /**
     * Send an error response packet to the client.
     */
    private void sendErrorResponse(final int errorCode, final String errorMessage) {
        sendErrorResponse(errorCode, null, errorMessage);
    }

    /**
     * Send an error response packet to the client with SQL state.
     */
    private void sendErrorResponse(final int errorCode, final String sqlState, final String errorMessage) {
        send(byteBufAllocator -> {
            var buffer = byteBufAllocator.buffer();
            var builder = ErrPacketBuilder
                    .create()
                    .withErrorCode((short) errorCode);
                    
            if (sqlState != null) {
                builder.withSqlState(sqlState);
            }
            
            if (errorMessage != null) {
                builder.withErrorMessage(errorMessage);
            }
            
            builder.withSequenceId((byte) sequenceId).build(buffer);
            return buffer;
        });
    }

    /**
     * Handle COM_INIT_DB command to switch databases.
     */
    private void handleInitDatabase(final String databaseName) {
        LOGGER.debug("Session[{}]: Switching to database: {}", id, databaseName);
        
        try {
            // Validate database exists by listing catalogs/databases
            var databases = spark.catalog().listDatabases().collectAsList();
            boolean databaseExists = false;
            
            for (var database : databases) {
                if (database.name().equals(databaseName)) {
                    databaseExists = true;
                    break;
                }
            }
            
            if (databaseExists || "information_schema".equalsIgnoreCase(databaseName)) {
                // Update current database
                this.currentDatabase = databaseName;
                
                // Send OK packet
                send(byteBufAllocator -> {
                    var buffer = byteBufAllocator.buffer();
                    OkPacketBuilder
                            .create()
                            .withHeader(OkPacketBuilder.OK_PACKET_HEADER)
                            .withSequenceId((byte) sequenceId)
                            .build(buffer);
                    return buffer;
                });
                
                LOGGER.debug("Session[{}]: Successfully switched to database: {}", id, databaseName);
            } else {
                // Database doesn't exist
                sendErrorResponse(1049, String.format("Unknown database '%s'", databaseName));
            }
        } catch (Exception e) {
            LOGGER.error("Session[{}]: Error switching to database '{}': {}", id, databaseName, e.getMessage(), e);
            sendErrorResponse(1105, "Error accessing database");
        }
    }

    /**
     * Check if a query is a USE statement.
     */
    private boolean isUseStatement(final String query) {
        String trimmed = query.trim();
        return trimmed.toLowerCase().startsWith("use ");
    }
    
    /**
     * Handle USE database statement by parsing the database name and switching to it.
     */
    private StreamingTextResultsetPacketBuilder handleUseStatement(final String query) {
        String trimmed = query.trim();
        String[] parts = trimmed.split("\\s+", 2);
        
        if (parts.length != 2) {
            throw new RuntimeException("Invalid USE statement syntax");
        }
        
        String databaseName = parts[1];
        // Remove quotes if present
        if ((databaseName.startsWith("'") && databaseName.endsWith("'")) ||
            (databaseName.startsWith("`") && databaseName.endsWith("`")) ||
            (databaseName.startsWith("\"") && databaseName.endsWith("\""))) {
            databaseName = databaseName.substring(1, databaseName.length() - 1);
        }
        
        LOGGER.debug("Session[{}]: USE statement - switching to database: {}", id, databaseName);
        
        // Validate database exists
        var databases = spark.catalog().listDatabases().collectAsList();
        boolean databaseExists = false;
        
        for (var database : databases) {
            if (database.name().equals(databaseName)) {
                databaseExists = true;
                break;
            }
        }
        
        if (databaseExists || "information_schema".equalsIgnoreCase(databaseName)) {
            this.currentDatabase = databaseName;
            LOGGER.debug("Session[{}]: Successfully switched to database: {}", id, databaseName);
            
            // Return empty result set for successful USE statement
            return createEmptyResultSet();
        } else {
            throw new RuntimeException(String.format("Unknown database '%s'", databaseName));
        }
    }
    
    /**
     * Create an empty result set for commands that don't return data.
     */
    private StreamingTextResultsetPacketBuilder createEmptyResultSet() {
        // Create a dataset with no columns and no rows
        var emptySchema = new StructType(new StructField[0]);
        var emptyDataset = spark.createDataFrame(Lists.newArrayList(), emptySchema);
        return StreamingTextResultsetPacketBuilder.create(this.handshakeResponse.clientFlag(), emptyDataset);
    }

    /**
     * Load query timeout from environment variable or use default.
     */
    private static Duration loadQueryTimeout() {
        String value = System.getenv(ENV_QUERY_TIMEOUT_MINUTES);
        if (value != null) {
            try {
                return Duration.ofMinutes(Long.parseLong(value));
            } catch (NumberFormatException e) {
                LOGGER.warn("Invalid {} value '{}', using default {}", ENV_QUERY_TIMEOUT_MINUTES, value, DEFAULT_QUERY_TIMEOUT);
            }
        }
        return DEFAULT_QUERY_TIMEOUT;
    }

}
