package io.exsql.mysql.server.protocol.client;

import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

public record ComPacket(PacketHeader header, Object body) {
    public static ComPacket parse(final ByteBuf buffer) throws IllegalArgumentException {
        var header = PacketHeader.parse(buffer, true);
        return switch (header.kind()) {
            case QUERY -> new ComPacket(header, StringEncoding.decodeFixedLength(buffer, header.payloadLength()));
            case QUIT -> new ComPacket(header, null);
            default -> throw new IllegalArgumentException("Unknown packet type: " + header.kind());
        };
    }

    public <T> T getBody(final Class<T> clazz) {
        return clazz.cast(body);
    }
}
