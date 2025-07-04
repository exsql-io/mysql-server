package io.exsql.mysql.server.protocol;

import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.exsql.mysql.server.protocol.datatypes.LengthEncodedInteger;
import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

// TODO:
//  - handle split of payload >= 16MB
//  - handle session state
public final class OkPacketBuilder extends PacketBuilder {

    public static final byte OK_PACKET_HEADER = (byte) 0x00;
    public static final byte EOF_PACKET_HEADER = (byte) 0xFE;

    private byte header;
    private long affectedRows;
    private long lastInsertId;
    private int statusFlags;
    private int warnings;
    private String info;

    private OkPacketBuilder() {}

    public static OkPacketBuilder create() {
        return new OkPacketBuilder();
    }

    public OkPacketBuilder withHeader(final byte header) {
        this.header = header;
        return this;
    }

    public OkPacketBuilder withAffectedRows(final long affectedRows) {
        this.affectedRows = affectedRows;
        return this;
    }

    public OkPacketBuilder withLastInsertId(final long lastInsertId) {
        this.lastInsertId = lastInsertId;
        return this;
    }

    public OkPacketBuilder withStatusFlags(final int statusFlags) {
        this.statusFlags = statusFlags;
        return this;
    }

    public OkPacketBuilder withWarnings(final int warnings) {
        this.warnings = warnings;
        return this;
    }

    public OkPacketBuilder withInfo(final String info) {
        this.info = info;
        return this;
    }

    @Override
    protected int buildPayload(final ByteBuf buffer) {
        int length = 0;

        // OK packet header (0x00 or 0xFE)
        length += FixedLengthInteger.encode1(buffer, header);

        // Affected rows (length-encoded integer)
        length += LengthEncodedInteger.encode(buffer, affectedRows);

        // Last insert ID (length-encoded integer)
        length += LengthEncodedInteger.encode(buffer, lastInsertId);

        // Status flags (2 bytes)
        length += FixedLengthInteger.encode2(buffer, statusFlags);

        // Warnings (2 bytes)
        length += FixedLengthInteger.encode2(buffer, warnings);

        // Info (string, optional)
        if (info != null && !info.isEmpty()) {
            var bytes = info.getBytes(StandardCharsets.UTF_8);
            StringEncoding.encodeFixedLength(buffer, bytes, bytes.length);
            length += bytes.length;
        }

        return length;
    }

}