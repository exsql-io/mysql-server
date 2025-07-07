package io.exsql.mysql.server.protocol.server;

import io.exsql.mysql.server.protocol.Capability;
import io.exsql.mysql.server.protocol.datatypes.FixedLengthInteger;
import io.exsql.mysql.server.protocol.datatypes.LengthEncodedInteger;
import io.exsql.mysql.server.protocol.datatypes.StringEncoding;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * Builder for COM_QUERY response packets.
 * This builder creates a complete COM_QUERY response sequence for a result set.
 * According to the MySQL protocol, a COM_QUERY response for a result set consists of:
 * 1. A result set header packet
 * 2. Column definition packets (one for each column)
 * 3. An EOF packet (if CLIENT_DEPRECATE_EOF is not set)
 * 4. Row data packets (one for each row)
 * 5. A final EOF packet (if CLIENT_DEPRECATE_EOF is not set) or OK packet
 */
public final class COMQueryResponseBuilder extends PacketBuilder {

    private final int clientFlag;
    private final List<ColumnDefinition> columnDefinitions = new ArrayList<>();
    private final List<List<String>> rows = new ArrayList<>();

    private COMQueryResponseBuilder(final int clientFlag) {
        this.clientFlag = clientFlag;
    }

    /**
     * Creates a new instance of the COMQueryResponseBuilder.
     *
     * @return a new COMQueryResponseBuilder instance
     */
    public static COMQueryResponseBuilder create(final int capabilitiesFlag) {
        return new COMQueryResponseBuilder(capabilitiesFlag);
    }

    /**
     * Adds a column definition to the result set.
     *
     * @param columnDefinition the column definition
     * @return this builder instance
     */
    public COMQueryResponseBuilder withColumnDefinition(final ColumnDefinition columnDefinition) {
        this.columnDefinitions.add(columnDefinition);
        return this;
    }

    /**
     * Adds a row to the result set.
     *
     * @param row the row data (list of column values)
     * @return this builder instance
     */
    public COMQueryResponseBuilder withRow(final List<String> row) {
        this.rows.add(row);
        return this;
    }

    /**
     * Builds the result set header packet payload.
     *
     * @param buffer the buffer to write the payload to
     * @return the length of the payload in bytes
     */
    @Override
    protected int buildPayload(final ByteBuf buffer) {
        int length = 0;

        if (Capability.hasClientOptionalResultsetMetadataFlag(this.clientFlag)) {
            length += FixedLengthInteger.encode1(buffer, 0x01);
        }

        length += LengthEncodedInteger.encode(buffer, columnDefinitions.size());
        if (columnDefinitions.isEmpty() && rows.isEmpty()) {
            return length;
        }

        if (!Capability.hasClientOptionalResultsetMetadataFlag(this.clientFlag) || !columnDefinitions.isEmpty()) {
            for (final ColumnDefinition columnDefinition: columnDefinitions) {
                length += columnDefinition.build(buffer);
            }
        }

        for (final List<String> row: rows) {
            length += buildRowDataPacket(buffer, row);
        }

        length += OkPacketBuilder
                .create()
                .withHeader(OkPacketBuilder.EOF_PACKET_HEADER)
                .build(buffer);

        return length;
    }

    /**
     * Builds a row data packet.
     *
     * @param buffer the buffer to write the packet to
     * @param row the row data (list of column values)
     * @return the length of the packet in bytes
     */
    private int buildRowDataPacket(final ByteBuf buffer, final List<String> row) {
        int length = 0;

        // Each column value is encoded as a length-encoded string
        for (final String value: row) {
            if (value == null) {
                // NULL value is encoded as 0xFB
                length += FixedLengthInteger.encode1(buffer, 0xFB);
            } else {
                // Non-NULL value is encoded as a length-encoded string
                length += StringEncoding.encodeLengthEncoded(buffer, value);
            }
        }

        return length;
    }

    /**
     * Represents a column definition in a result set.
     */
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

}
