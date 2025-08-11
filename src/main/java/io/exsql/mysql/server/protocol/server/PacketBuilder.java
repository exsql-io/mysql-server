package io.exsql.mysql.server.protocol.server;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.netty.buffer.ByteBuf;

public abstract class PacketBuilder {

    // MySQL maximum packet size (16MB - 1 byte)
    public static final int MAX_PACKET_SIZE = 0xFFFFFF; // 16777215

    private byte sequenceId;

    PacketBuilder() {}

    public PacketBuilder withSequenceId(final byte sequenceId) {
        this.sequenceId = sequenceId;
        return this;
    }

    protected abstract int buildPayload(final ByteBuf buffer);

    public int build(final ByteBuf buffer) {
        var writerIndex = buffer.writerIndex();

        int length = 0;

        // Reserve 3 bytes integer for payload size.
        length += FixedLengthInteger.encode3(buffer, 0);
        length += FixedLengthInteger.encode1(buffer, sequenceId);

        var payloadLength = buildPayload(buffer);
        
        // Validate packet size doesn't exceed MySQL limit
        int totalPacketSize = payloadLength + 4; // payload + 4 byte header
        if (totalPacketSize > MAX_PACKET_SIZE) {
            throw new IllegalStateException(
                String.format("Packet size %d bytes exceeds MySQL limit of %d bytes. " +
                             "Consider implementing chunking or reducing data size.", 
                             totalPacketSize, MAX_PACKET_SIZE));
        }
        
        if (payloadLength > 0) {
            buffer.writerIndex(writerIndex);
            FixedLengthInteger.encode3(buffer, payloadLength);
            buffer.writerIndex(buffer.writerIndex() + payloadLength + 1);
        }

        length += payloadLength;
        return length;
    }

}
