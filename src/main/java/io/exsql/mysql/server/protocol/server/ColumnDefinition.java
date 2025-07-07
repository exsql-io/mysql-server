package io.exsql.mysql.server.protocol.server;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.exsql.mysql.server.protocol.datatypes.LengthEncodedInteger;
import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

public record ColumnDefinition(
        String schema,
        String table,
        String originalTable,
        String name,
        String originalName,
        int characterSet,
        int columnLength,
        byte columnType,
        int flags,
        byte decimals
) {
    public int build(final ByteBuf buffer) {
        int length = 0;

        length += StringEncoding.encodeLengthEncoded(buffer, "def");
        length += StringEncoding.encodeLengthEncoded(buffer, this.schema);
        length += StringEncoding.encodeLengthEncoded(buffer, this.table);
        length += StringEncoding.encodeLengthEncoded(buffer, this.originalTable);
        length += StringEncoding.encodeLengthEncoded(buffer, this.name);
        length += StringEncoding.encodeLengthEncoded(buffer, this.originalName);

        // Fixed fields length (always 0x0C)
        length += LengthEncodedInteger.encode(buffer, 0x0C);
        length += FixedLengthInteger.encode2(buffer, this.characterSet);
        length += FixedLengthInteger.encode4(buffer, this.columnLength);
        length += FixedLengthInteger.encode1(buffer, this.columnType);
        length += FixedLengthInteger.encode2(buffer, this.flags);
        length += FixedLengthInteger.encode1(buffer, this.decimals);

        // Reserved (2 bytes of 0)
        length += FixedLengthInteger.encode2(buffer, 0);

        return length;
    }
}
