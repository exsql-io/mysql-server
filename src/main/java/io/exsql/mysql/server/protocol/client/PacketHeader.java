package io.exsql.mysql.server.protocol.client;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.netty.buffer.ByteBuf;

public record PacketHeader(int payloadLength, byte sequenceId) {
    public static PacketHeader parse(final ByteBuf buffer) {
        return new PacketHeader(FixedLengthInteger.decode3(buffer), (byte) FixedLengthInteger.decode1(buffer));
    }
}
