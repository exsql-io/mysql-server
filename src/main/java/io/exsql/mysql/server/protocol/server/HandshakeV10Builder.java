package io.exsql.mysql.server.protocol.server;

import io.exsql.mysql.server.protocol.Capability;
import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;

public class HandshakeV10Builder extends PacketBuilder {

    private final static byte PROTOCOL_VERSION = 10;

    private final static byte[] SERVER_VERSION = "9.3.0-optable".getBytes(StandardCharsets.US_ASCII);

    private final static byte FILTER = 0x00;

    private final static int CAPABILITIES_FLAG = Capability.buildCapabilitiesFlag();

    private final static byte BINARY_CHARSET_ID = 0x3F;

    private final static byte[] RESERVED = new byte[10];

    private final static String AUTH_PLUGIN_NAME = "mysql_clear_password";

    private final static byte[] AUTH_PLUGIN_NAME_BYTES = AUTH_PLUGIN_NAME.getBytes(StandardCharsets.US_ASCII);

    private final int sessionId;

    private final byte[] authPluginDataPart1;

    private final byte[] authPluginDataPart2;

    private HandshakeV10Builder(final int sessionId) {
        this.sessionId = sessionId;

        // Generate random authentication data
        SecureRandom random = new SecureRandom();
        this.authPluginDataPart1 = new byte[8];
        random.nextBytes(this.authPluginDataPart1);

        this.authPluginDataPart2 = new byte[12]; // 12 bytes + null terminator = 13 bytes
        random.nextBytes(this.authPluginDataPart2);
    }

    public static HandshakeV10Builder create(final int sessionId) {
        return new HandshakeV10Builder(sessionId);
    }

    @Override
    protected int buildPayload(final ByteBuf buffer) {
        var length = 0;
        length += FixedLengthInteger.encode1(buffer, PROTOCOL_VERSION);
        length += StringEncoding.encodeNullTerminated(buffer, SERVER_VERSION);
        length += FixedLengthInteger.encode4(buffer, sessionId);
        length += StringEncoding.encodeFixedLength(buffer, authPluginDataPart1, authPluginDataPart1.length);
        length += FixedLengthInteger.encode1(buffer, FILTER);

        final short capabilitiesFlagLowBytes = (short) (CAPABILITIES_FLAG & 0xFFFF);
        length += FixedLengthInteger.encode2(buffer, capabilitiesFlagLowBytes);
        length += FixedLengthInteger.encode1(buffer, BINARY_CHARSET_ID);
        length += FixedLengthInteger.encode2(buffer, 0);

        final short capabilitiesFlagHighBytes = (short) (CAPABILITIES_FLAG >> 16);
        length += FixedLengthInteger.encode2(buffer, capabilitiesFlagHighBytes);
        length += FixedLengthInteger.encode1(buffer, 20); // Length of auth plugin data (8 + 12)
        length += StringEncoding.encodeFixedLength(buffer, RESERVED, RESERVED.length);
        length += StringEncoding.encodeFixedLength(buffer, authPluginDataPart2, authPluginDataPart2.length);
        length += StringEncoding.encodeNullTerminated(buffer, AUTH_PLUGIN_NAME_BYTES);

        return length;
    }

}
