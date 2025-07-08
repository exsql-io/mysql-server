package io.exsql.mysql.server.protocol.client;

import io.exsql.mysql.server.protocol.Capability;
import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

public record HandshakeResponse41Packet(PacketHeader header,
                                        int clientFlag,
                                        int maxPacketSize,
                                        short characterSet,
                                        boolean isSSLRequestPacket,
                                        String authenticationPlugin,
                                        String username,
                                        String authentication,
                                        String database) {

    public final static String DEFAULT_DATABASE_NAME = "default";

    private final static int FILLER_LENGTH = 23;

    public static HandshakeResponse41Packet parse(final ByteBuf buffer) throws IllegalArgumentException {
        var header = PacketHeader.parse(buffer, false);
        var clientFlag = FixedLengthInteger.decode4(buffer);
        var maxPacketSize = FixedLengthInteger.decode4(buffer);
        var characterSet = FixedLengthInteger.decode1(buffer);
        // skip filler string[23]
        buffer.skipBytes(FILLER_LENGTH);

        // if the client has the SSL capability and the buffer is empty after reading the filler,
        // this is probably the SSLRequest packet.
        var isSSLPacket = Capability.hasClientSSLFlag(clientFlag) && buffer.readableBytes() == 0;
        if (isSSLPacket) {
            return new HandshakeResponse41Packet(header, clientFlag, maxPacketSize, characterSet, true, "", "", "", "");
        }

        var username = StringEncoding.decodeNullTerminated(buffer);
        var authentication = HandshakeResponse41Packet.parseAuthResponse(buffer, clientFlag);

        var database = DEFAULT_DATABASE_NAME;
        if (Capability.hasClientConnectWithDBFlag(clientFlag)) {
            database = StringEncoding.decodeNullTerminated(buffer);
        }

        var authenticationPlugin = "";
        if (Capability.hasClientPluginAuthClientDataFlag(clientFlag)) {
            authenticationPlugin = StringEncoding.decodeNullTerminated(buffer);
        }

        buffer.skipBytes(buffer.readableBytes());

        return new HandshakeResponse41Packet(header, clientFlag, maxPacketSize, characterSet, false, authenticationPlugin, username, authentication, database);
    }

    private static String parseAuthResponse(final ByteBuf buffer, final int clientFlag) {
        if (Capability.hasClientPluginAuthLenencClientDataFlag(clientFlag)) {
            return StringEncoding.decodeLengthEncoded(buffer);
        }

        var length = FixedLengthInteger.decode1(buffer);
        return StringEncoding.decodeFixedLength(buffer, length);
    }

}
