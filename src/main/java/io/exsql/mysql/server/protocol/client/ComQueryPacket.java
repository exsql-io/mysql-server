package io.exsql.mysql.server.protocol.client;

import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

public record ComQueryPacket(PacketHeader header, String query) {
    public static ComQueryPacket parse(final ByteBuf buffer) throws IllegalArgumentException {
        var header = PacketHeader.parse(buffer, true);
        var query = StringEncoding.decodeFixedLength(buffer, header.payloadLength());
        return new ComQueryPacket(header, query);
    }
}
