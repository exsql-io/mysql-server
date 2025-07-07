package io.exsql.mysql.server.protocol;

import com.google.common.collect.Lists;
import io.exsql.mysql.server.protocol.client.COMQueryPacket;
import io.exsql.mysql.server.protocol.client.HandshakeResponse41;
import io.exsql.mysql.server.protocol.server.COMQueryResponseDatasetHelper;
import io.exsql.mysql.server.protocol.server.ErrPacketBuilder;
import io.exsql.mysql.server.protocol.server.HandshakeV10Builder;
import io.exsql.mysql.server.protocol.server.OkPacketBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.classic.Dataset;
import org.apache.spark.sql.classic.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.util.function.Function;

public class Session {

    private static final Logger LOGGER = LoggerFactory.getLogger(Session.class);

    private static final Row[] VERSION_COMMENT_ROW = new GenericRow[] {
            new GenericRow(new String[] { "data-platform-mysql-frontend" })
    };

    private static final StructType VERSION_COMMENT_SCHEMA = StructType.fromDDL("version_comment string");

    private static final Encoder<Row> VERSION_COMMENT_ENCODER = Encoders.row(VERSION_COMMENT_SCHEMA);

    private final SparkSession spark;

    private final int id;

    private final Connection connection;

    private SessionPhase phase;

    private short sequenceId;

    private HandshakeResponse41 handshakeResponse;

    public Session(final SparkSession spark, final int id, final Connection connection) {
        this.spark = spark;
        this.id = id;
        this.connection = connection;
        this.phase = SessionPhase.CONNECTION_PHASE_INITIAL_HANDSHAKE;
    }

    public void initialize() {
        this.send(byteBufAllocator -> {
            var buffer = byteBufAllocator.buffer();
            HandshakeV10Builder.create(this.id).withSequenceId((byte) sequenceId).build(buffer);
            return buffer;
        });

        this.waitForResponse();
    }

    public void send(final Function<ByteBufAllocator, ByteBuf> producer) {
        var buffer = producer.apply(connection.outbound().alloc());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Session[{}]: sending: \n{}", id, ByteBufUtil.prettyHexDump(buffer));
        }

        connection
                .outbound()
                .send(Mono.just(buffer))
                .then()
                .subscribe()
                .dispose();

        incrementSequenceId();
    }

    public void close() {
        LOGGER.debug("Closing session[{}]: {}:{}", id, connection.channel().remoteAddress(), connection.channel().localAddress());
        connection.disposeNow();
    }

    private void waitForResponse() {
        connection
                .inbound()
                .receive()
                .doOnEach(signal -> {
                    var inbound = signal.get();
                    incrementSequenceId();

                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("Session[{}]: received: \n{}", id, inbound == null ? "null": ByteBufUtil.prettyHexDump(inbound));
                    }

                    switch (phase) {
                        case CONNECTION_PHASE_INITIAL_HANDSHAKE:
                            // Consume it for now, will need to validate the user later.
                            this.handshakeResponse = HandshakeResponse41.parse(inbound);
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
                            break;
                        case COMMAND_PHASE:
                            var cq = COMQueryPacket.parse(inbound);
                            resetSequenceId(cq.header().sequenceId());

                            try {
                                Dataset<Row> ds = null;
                                if (cq.query().equalsIgnoreCase("SELECT @@version_comment LIMIT 1")) {
                                    ds = spark.createDataset(Lists.newArrayList(VERSION_COMMENT_ROW), VERSION_COMMENT_ENCODER);
                                } else {
                                    ds = spark.sql(cq.query());
                                }

                                var stream = COMQueryResponseDatasetHelper.fromDataset(this.handshakeResponse.clientFlag(), ds);
                                while (stream.hasNext()) {
                                    send(byteBufAllocator -> {
                                        var buffer = byteBufAllocator.buffer();
                                        stream.next().withSequenceId((byte) sequenceId).build(buffer);
                                        return buffer;
                                    });
                                }
                            } catch (final Throwable throwable) {
                                if (LOGGER.isDebugEnabled()) {
                                    LOGGER.debug("Session[{}]: an error occurred while processing query: {}. See logs for more details.", id, cq.query(), throwable);
                                }

                                switch (throwable) {
                                    case AnalysisException ae ->
                                        // Error number: 1064; Symbol: ER_PARSE_ERROR; SQLSTATE: 42000
                                        send(byteBufAllocator -> {
                                            var buffer = byteBufAllocator.buffer();
                                            ErrPacketBuilder
                                                    .create()
                                                    .withErrorCode((short) 1064)
                                                    .withSqlState("42000")
                                                    .withErrorMessage(ae.getSimpleMessage())
                                                    .withSequenceId((byte) sequenceId)
                                                    .build(buffer);
                                            return buffer;
                                        });
                                    default ->
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

}
