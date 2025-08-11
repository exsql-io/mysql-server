package io.exsql.mysql.server.protocol.client;

public enum PacketHeaderKind {
    QUIT((byte) 0x01), INIT_DB((byte) 0x02), QUERY((byte) 0x03), UNKNOWN((byte) 0xFF);

    private final byte value;

    PacketHeaderKind(final byte value) {
        this.value = value;
    }

    public static PacketHeaderKind fromValue(final byte value) {
        for (final PacketHeaderKind kind: PacketHeaderKind.values()) {
            if (kind.value == value) {
                return kind;
            }
        }

        throw new IllegalArgumentException("Unknown packet header kind: " + value);
    }

}
