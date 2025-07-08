package io.exsql.mysql.server.protocol.client;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.netty.buffer.ByteBuf;

public record PacketHeader(int payloadLength, byte sequenceId, PacketHeaderKind kind) {
    public static PacketHeader parse(final ByteBuf buffer, final boolean hasKind) {
        var payloadLength = FixedLengthInteger.decode3(buffer);
        return new PacketHeader(
                hasKind ? payloadLength - 1 : payloadLength,
                (byte) FixedLengthInteger.decode1(buffer),
                hasKind ? PacketHeaderKind.fromValue((byte) FixedLengthInteger.decode1(buffer)) : PacketHeaderKind.UNKNOWN
        );
    }
}
