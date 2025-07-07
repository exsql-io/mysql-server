package io.exsql.mysql.server.protocol.server;

import io.exsql.mysql.server.protocol.Capability;
import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.exsql.mysql.server.protocol.datatypes.LengthEncodedInteger;
import io.netty.buffer.ByteBuf;

final class ColumnCountPacketBuilder extends PacketBuilder {
    private final int clientFlag;
    private final int columnCount;

    ColumnCountPacketBuilder(final int clientFlag, final int columnCount) {
        this.clientFlag = clientFlag;
        this.columnCount = columnCount;
    }

    static ColumnCountPacketBuilder create(final int clientFlag, final int columnCount) {
        return new ColumnCountPacketBuilder(clientFlag, columnCount);
    }

    @Override
    protected int buildPayload(final ByteBuf buffer) {
        int length = 0;

        if (Capability.hasClientOptionalResultsetMetadataFlag(this.clientFlag)) {
            length += FixedLengthInteger.encode1(buffer, 0x01);
        }

        length += LengthEncodedInteger.encode(buffer, this.columnCount);
        return length;
    }
}
