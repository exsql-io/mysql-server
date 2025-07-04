package io.exsql.mysql.server.protocol;

import io.exsql.mysql.server.protocol.client.HandshakeResponse41;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;

import java.util.function.Function;

public class Session {

    private static final Logger LOGGER = LoggerFactory.getLogger(Session.class);

    private final int id;

    private final Connection connection;

    private SessionPhase phase;

    private short sequenceId;

    public Session(final int id, final Connection connection) {
        this.id = id;
        this.connection = connection;
        this.phase = SessionPhase.CONNECTION_PHASE_INITIAL_HANDSHAKE;
    }

    public void initialize() {
        this.send(byteBufAllocator -> {
            var buffer = byteBufAllocator.buffer();
            return HandshakeV10Builder.create(this.id).withSequenceId((byte) sequenceId).build(buffer);
        });

        this.waitForResponse();
    }

    public void send(final Function<ByteBufAllocator, ByteBuf> producer) {
        connection
                .outbound()
                .send(Mono.just(producer.apply(connection.outbound().alloc())))
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
                            HandshakeResponse41.parse(inbound);
                            send(byteBufAllocator -> {
                                var buffer = byteBufAllocator.buffer();
                                return OkPacketBuilder
                                        .create()
                                        .withHeader(OkPacketBuilder.OK_PACKET_HEADER)
                                        .withStatusFlags(0)
                                        .withSequenceId((byte) sequenceId)
                                        .build(buffer);
                            });
                            phase = SessionPhase.COMMAND_PHASE;
                            break;
                        case COMMAND_PHASE:
                            send(byteBufAllocator -> {
                                var buffer = byteBufAllocator.buffer();
                                return OkPacketBuilder
                                        .create()
                                        .withHeader(OkPacketBuilder.OK_PACKET_HEADER)
                                        .withStatusFlags(0)
                                        .withSequenceId((byte) sequenceId)
                                        .build(buffer);
                            });
                            break;
                    }
                })
                .then().subscribe(connection.disposeSubscriber());
    }

    private void incrementSequenceId() {
        sequenceId++;
        if (sequenceId > 0xFF) {
            sequenceId = 0;
        }
    }

}
