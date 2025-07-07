package io.exsql.mysql.server.protocol.server;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

import java.util.Iterator;
import java.util.List;

class RowDataPacketStreamBuilder implements Iterator<PacketBuilder> {
    private final Iterator<List<String>> rows;

    RowDataPacketStreamBuilder(final Iterator<List<String>> rows) {
        this.rows = rows;
    }

    static RowDataPacketStreamBuilder create(final List<List<String>> rows) {
        return new RowDataPacketStreamBuilder(rows.iterator());
    }

    @Override
    public boolean hasNext() {
        return this.rows.hasNext();
    }

    @Override
    public PacketBuilder next() {
        return new RowDataPacketBuilder(this.rows.next());
    }

    static class RowDataPacketBuilder extends PacketBuilder {
        private final List<String> row;
        RowDataPacketBuilder(final List<String> row) {
            this.row = row;
        }

        @Override
        protected int buildPayload(final ByteBuf buffer) {
            int length = 0;

            // Each column value is encoded as a length-encoded string
            for (final String value: row) {
                if (value == null) {
                    // NULL value is encoded as 0xFB
                    length += FixedLengthInteger.encode1(buffer, 0xFB);
                } else {
                    // Non-NULL value is encoded as a length-encoded string
                    length += StringEncoding.encodeLengthEncoded(buffer, value);
                }
            }

            return length;
        }
    }

}
