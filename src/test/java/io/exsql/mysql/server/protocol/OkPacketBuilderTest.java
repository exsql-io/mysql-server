package io.exsql.mysql.server.protocol;

import io.exsql.mysql.server.protocol.datatypes.LengthEncodedInteger;
import io.exsql.mysql.server.protocol.server.OkPacketBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OkPacketBuilderTest {

    @Test
    @DisplayName("Test building a minimal OK packet with only affected rows")
    void testBuildMinimalOkPacket() {
        final ByteBuf buffer = Unpooled.buffer();
        final long affectedRows = 5;
        final byte sequenceId = 1;

        OkPacketBuilder
                .create()
                .withAffectedRows(affectedRows)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // OK packet header
        expectedPayloadLength += 1; // Affected rows (1 byte for small values)
        expectedPayloadLength += 1; // Last insert ID (default 0, 1 byte)
        expectedPayloadLength += 2; // Status flags
        expectedPayloadLength += 2; // Warnings

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(OkPacketBuilder.OK_PACKET_HEADER, buffer.readByte()); // OK packet header (0x00)
        assertEquals(affectedRows, LengthEncodedInteger.decode(buffer)); // Affected rows
        assertEquals(0, LengthEncodedInteger.decode(buffer)); // Last insert ID (default 0)
        assertEquals(0, buffer.readShortLE()); // Status flags (default 0)
        assertEquals(0, buffer.readShortLE()); // Warnings (default 0)

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building a complete OK packet with all fields")
    void testBuildCompleteOkPacket() {
        final ByteBuf buffer = Unpooled.buffer();
        final long affectedRows = 10;
        final long lastInsertId = 42;
        final int statusFlags = 0x0002; // SERVER_STATUS_AUTOCOMMIT
        final int warnings = 1;
        final String info = "Records affected: 10";
        final byte sequenceId = 2;

        OkPacketBuilder
                .create()
                .withAffectedRows(affectedRows)
                .withLastInsertId(lastInsertId)
                .withStatusFlags(statusFlags)
                .withWarnings(warnings)
                .withInfo(info)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // OK packet header
        expectedPayloadLength += 1; // Affected rows (1 byte for small values)
        expectedPayloadLength += 1; // Last insert ID (1 byte for small values)
        expectedPayloadLength += 2; // Status flags
        expectedPayloadLength += 2; // Warnings
        expectedPayloadLength += info.getBytes(StandardCharsets.UTF_8).length; // Info

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(OkPacketBuilder.OK_PACKET_HEADER, buffer.readByte()); // OK packet header (0x00)
        assertEquals(affectedRows, LengthEncodedInteger.decode(buffer)); // Affected rows
        assertEquals(lastInsertId, LengthEncodedInteger.decode(buffer)); // Last insert ID
        assertEquals(statusFlags, buffer.readShortLE()); // Status flags
        assertEquals(warnings, buffer.readShortLE()); // Warnings

        // Verify info
        byte[] infoBytes = new byte[info.getBytes(StandardCharsets.UTF_8).length];
        buffer.readBytes(infoBytes);
        assertEquals(info, new String(infoBytes, StandardCharsets.UTF_8));

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building an OK packet with large affected rows and last insert ID")
    void testBuildOkPacketWithLargeValues() {
        final ByteBuf buffer = Unpooled.buffer();
        final long affectedRows = 70000; // Requires 3 bytes (0xFD + 3 bytes)
        final long lastInsertId = 16777217; // Requires 9 bytes (0xFE + 8 bytes)
        final byte sequenceId = 3;

        OkPacketBuilder
                .create()
                .withAffectedRows(affectedRows)
                .withLastInsertId(lastInsertId)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // OK packet header
        expectedPayloadLength += 4; // Affected rows (1 byte prefix + 3 bytes)
        expectedPayloadLength += 9; // Last insert ID (1 byte prefix + 8 bytes)
        expectedPayloadLength += 2; // Status flags
        expectedPayloadLength += 2; // Warnings

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(OkPacketBuilder.OK_PACKET_HEADER, buffer.readByte()); // OK packet header (0x00)
        assertEquals(affectedRows, LengthEncodedInteger.decode(buffer)); // Affected rows
        assertEquals(lastInsertId, LengthEncodedInteger.decode(buffer)); // Last insert ID
        assertEquals(0, buffer.readShortLE()); // Status flags (default 0)
        assertEquals(0, buffer.readShortLE()); // Warnings (default 0)

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building an OK packet with empty info")
    void testBuildOkPacketWithEmptyInfo() {
        final ByteBuf buffer = Unpooled.buffer();
        final long affectedRows = 5;
        final String info = "";
        final byte sequenceId = 4;

        OkPacketBuilder
                .create()
                .withAffectedRows(affectedRows)
                .withInfo(info)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // OK packet header
        expectedPayloadLength += 1; // Affected rows (1 byte for small values)
        expectedPayloadLength += 1; // Last insert ID (default 0, 1 byte)
        expectedPayloadLength += 2; // Status flags
        expectedPayloadLength += 2; // Warnings
        // No info bytes since info is empty

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(OkPacketBuilder.OK_PACKET_HEADER, buffer.readByte()); // OK packet header (0x00)
        assertEquals(affectedRows, LengthEncodedInteger.decode(buffer)); // Affected rows
        assertEquals(0, LengthEncodedInteger.decode(buffer)); // Last insert ID (default 0)
        assertEquals(0, buffer.readShortLE()); // Status flags (default 0)
        assertEquals(0, buffer.readShortLE()); // Warnings (default 0)

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building an OK packet with non-ASCII characters in info")
    void testBuildOkPacketWithNonAsciiInfo() {
        final ByteBuf buffer = Unpooled.buffer();
        final long affectedRows = 5;
        final String info = "Records affected: 5 (你好, 世界!)";
        final byte sequenceId = 5;

        OkPacketBuilder
                .create()
                .withAffectedRows(affectedRows)
                .withInfo(info)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // OK packet header
        expectedPayloadLength += 1; // Affected rows (1 byte for small values)
        expectedPayloadLength += 1; // Last insert ID (default 0, 1 byte)
        expectedPayloadLength += 2; // Status flags
        expectedPayloadLength += 2; // Warnings
        expectedPayloadLength += info.getBytes(StandardCharsets.UTF_8).length; // Info

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(OkPacketBuilder.OK_PACKET_HEADER, buffer.readByte()); // OK packet header (0x00)
        assertEquals(affectedRows, LengthEncodedInteger.decode(buffer)); // Affected rows
        assertEquals(0, LengthEncodedInteger.decode(buffer)); // Last insert ID (default 0)
        assertEquals(0, buffer.readShortLE()); // Status flags (default 0)
        assertEquals(0, buffer.readShortLE()); // Warnings (default 0)

        // Verify info
        byte[] infoBytes = new byte[info.getBytes(StandardCharsets.UTF_8).length];
        buffer.readBytes(infoBytes);
        assertEquals(info, new String(infoBytes, StandardCharsets.UTF_8));

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

}