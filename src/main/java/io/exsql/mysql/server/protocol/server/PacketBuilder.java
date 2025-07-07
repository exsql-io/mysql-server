package io.exsql.mysql.server.protocol.server;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.netty.buffer.ByteBuf;

public abstract class PacketBuilder {

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
        if (payloadLength > 0) {
            buffer.writerIndex(writerIndex);
            FixedLengthInteger.encode3(buffer, payloadLength);
            buffer.writerIndex(buffer.writerIndex() + payloadLength + 1);
        }

        length += payloadLength;
        return length;
    }

}
