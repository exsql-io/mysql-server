package io.exsql.mysql.server.protocol;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.netty.buffer.ByteBuf;

abstract class PacketBuilder {

    private byte sequenceId;

    PacketBuilder() {}

    public PacketBuilder withSequenceId(final byte sequenceId) {
        this.sequenceId = sequenceId;
        return this;
    }

    protected abstract int buildPayload(final ByteBuf buffer);

    public ByteBuf build(final ByteBuf buffer) {
        buffer.markWriterIndex();

        // Reserve 3 bytes integer for payload size.
        FixedLengthInteger.encode3(buffer, 0);
        FixedLengthInteger.encode1(buffer, sequenceId);
        var length = buildPayload(buffer);
        if (length > 0) {
            buffer.resetWriterIndex();
            FixedLengthInteger.encode3(buffer, length);
            buffer.writerIndex(buffer.writerIndex() + length + 1);
        }

        return buffer;
    }

}
