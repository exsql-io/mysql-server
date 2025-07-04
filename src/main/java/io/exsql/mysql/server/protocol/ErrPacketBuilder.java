package io.exsql.mysql.server.protocol;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

public final class ErrPacketBuilder extends PacketBuilder {

    public static final byte ERR_PACKET_HEADER = (byte) 0xFF;

    private static final int SQL_STATE_MARKER_LENGTH = 1;

    private static final int SQL_STATE_LENGTH = 5;

    private short errorCode;

    private String sqlStateMarker;

    private String sqlState;

    private String errorMessage;

    private ErrPacketBuilder() {}

    public static ErrPacketBuilder create() {
        return new ErrPacketBuilder();
    }

    public ErrPacketBuilder withErrorCode(final short errorCode) {
        this.errorCode = errorCode;
        return this;
    }

    public ErrPacketBuilder withSqlStateMarker(final String sqlStateMarker) {
        this.sqlStateMarker = sqlStateMarker;
        return this;
    }

    public ErrPacketBuilder withSqlState(final String sqlState) {
        this.sqlState = sqlState;
        return this;
    }

    public ErrPacketBuilder withErrorMessage(final String errorMessage) {
        this.errorMessage = errorMessage;
        return this;
    }

    protected int buildPayload(final ByteBuf buffer) {
        int length = 0;

        FixedLengthInteger.encode1(buffer, ERR_PACKET_HEADER);
        length += 1;

        FixedLengthInteger.encode2(buffer, errorCode);
        length += 2;

        if (sqlStateMarker != null) {
            StringEncoding.encodeFixedLength(buffer, sqlStateMarker, SQL_STATE_MARKER_LENGTH);
            length += SQL_STATE_MARKER_LENGTH;
        }

        if (sqlState != null) {
            StringEncoding.encodeFixedLength(buffer, sqlState, SQL_STATE_LENGTH);
            length += SQL_STATE_LENGTH;
        }

        if (errorMessage != null) {
            var bytes = errorMessage.getBytes(StandardCharsets.UTF_8);

            StringEncoding.encodeFixedLength(buffer, bytes, bytes.length);
            length += bytes.length;
        }

        return length;
    }

}
