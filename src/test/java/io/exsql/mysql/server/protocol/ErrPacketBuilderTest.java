package io.exsql.mysql.server.protocol;

import io.exsql.mysql.server.protocol.server.ErrPacketBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ErrPacketBuilderTest {

    @Test
    @DisplayName("Test building a minimal error packet with only error code")
    void testBuildMinimalErrorPacket() {
        final ByteBuf buffer = Unpooled.buffer();
        final short errorCode = 1234;
        final byte sequenceId = 1;

        ErrPacketBuilder
                .create()
                .withErrorCode(errorCode)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(3, buffer.readUnsignedMediumLE()); // Payload length (1 byte header + 2 bytes error code)
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID
        assertEquals(ErrPacketBuilder.ERR_PACKET_HEADER, buffer.readByte()); // Error packet header (0xFF)

        // Verify error code
        assertEquals(errorCode, buffer.readShortLE()); // Error code

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building a complete error packet with all fields")
    void testBuildCompleteErrorPacket() {
        final ByteBuf buffer = Unpooled.buffer();
        final short errorCode = 1234;
        final String sqlStateMarker = "#";
        final String sqlState = "HY000";
        final String errorMessage = "Test error message";
        final byte sequenceId = 2;

        ErrPacketBuilder
                .create()
                .withErrorCode(errorCode)
                .withSqlStateMarker(sqlStateMarker)
                .withSqlState(sqlState)
                .withErrorMessage(errorMessage)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // Error packet header
        expectedPayloadLength += 2; // Error code
        expectedPayloadLength += 1; // SQL state marker
        expectedPayloadLength += 5; // SQL state
        expectedPayloadLength += errorMessage.getBytes(StandardCharsets.UTF_8).length; // Error message

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(ErrPacketBuilder.ERR_PACKET_HEADER, buffer.readByte()); // Error packet header (0xFF)
        assertEquals(errorCode, buffer.readShortLE()); // Error code

        // Verify SQL state marker
        byte[] sqlStateMarkerBytes = new byte[1];
        buffer.readBytes(sqlStateMarkerBytes);
        assertEquals(sqlStateMarker, new String(sqlStateMarkerBytes, StandardCharsets.UTF_8));

        // Verify SQL state
        byte[] sqlStateBytes = new byte[5];
        buffer.readBytes(sqlStateBytes);
        assertEquals(sqlState, new String(sqlStateBytes, StandardCharsets.UTF_8));

        // Verify error message
        byte[] errorMessageBytes = new byte[errorMessage.getBytes(StandardCharsets.UTF_8).length];
        buffer.readBytes(errorMessageBytes);
        assertEquals(errorMessage, new String(errorMessageBytes, StandardCharsets.UTF_8));

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building an error packet with only error code and message")
    void testBuildErrorPacketWithCodeAndMessage() {
        final ByteBuf buffer = Unpooled.buffer();
        final short errorCode = 1234;
        final String errorMessage = "Test error message";
        final byte sequenceId = 3;

        ErrPacketBuilder
                .create()
                .withErrorCode(errorCode)
                .withErrorMessage(errorMessage)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // Error packet header
        expectedPayloadLength += 2; // Error code
        expectedPayloadLength += errorMessage.getBytes(StandardCharsets.UTF_8).length; // Error message

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(ErrPacketBuilder.ERR_PACKET_HEADER, buffer.readByte()); // Error packet header (0xFF)
        assertEquals(errorCode, buffer.readShortLE()); // Error code

        // Verify error message
        byte[] errorMessageBytes = new byte[errorMessage.getBytes(StandardCharsets.UTF_8).length];
        buffer.readBytes(errorMessageBytes);
        assertEquals(errorMessage, new String(errorMessageBytes, StandardCharsets.UTF_8));

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building an error packet with SQL state but no marker")
    void testBuildErrorPacketWithSqlStateNoMarker() {
        final ByteBuf buffer = Unpooled.buffer();
        final short errorCode = 1234;
        final String sqlState = "HY000";
        final byte sequenceId = 4;

        ErrPacketBuilder
                .create()
                .withErrorCode(errorCode)
                .withSqlState(sqlState)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // Error packet header
        expectedPayloadLength += 2; // Error code
        expectedPayloadLength += 5; // SQL state

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(ErrPacketBuilder.ERR_PACKET_HEADER, buffer.readByte()); // Error packet header (0xFF)
        assertEquals(errorCode, buffer.readShortLE()); // Error code

        // Verify SQL state
        byte[] sqlStateBytes = new byte[5];
        buffer.readBytes(sqlStateBytes);
        assertEquals(sqlState, new String(sqlStateBytes, StandardCharsets.UTF_8));

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building an error packet with marker but no SQL state")
    void testBuildErrorPacketWithMarkerNoSqlState() {
        final ByteBuf buffer = Unpooled.buffer();
        final short errorCode = 1234;
        final String sqlStateMarker = "#";
        final byte sequenceId = 5;

        ErrPacketBuilder
                .create()
                .withErrorCode(errorCode)
                .withSqlStateMarker(sqlStateMarker)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // Error packet header
        expectedPayloadLength += 2; // Error code
        expectedPayloadLength += 1; // SQL state marker

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(ErrPacketBuilder.ERR_PACKET_HEADER, buffer.readByte()); // Error packet header (0xFF)
        assertEquals(errorCode, buffer.readShortLE()); // Error code

        // Verify SQL state marker
        byte[] sqlStateMarkerBytes = new byte[1];
        buffer.readBytes(sqlStateMarkerBytes);
        assertEquals(sqlStateMarker, new String(sqlStateMarkerBytes, StandardCharsets.UTF_8));

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building an error packet with empty error message")
    void testBuildErrorPacketWithEmptyErrorMessage() {
        final ByteBuf buffer = Unpooled.buffer();
        final short errorCode = 1234;
        final String errorMessage = "";
        final byte sequenceId = 6;

        ErrPacketBuilder
                .create()
                .withErrorCode(errorCode)
                .withErrorMessage(errorMessage)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // Error packet header
        expectedPayloadLength += 2; // Error code

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(ErrPacketBuilder.ERR_PACKET_HEADER, buffer.readByte()); // Error packet header (0xFF)
        assertEquals(errorCode, buffer.readShortLE()); // Error code

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building an error packet with non-ASCII characters in error message")
    void testBuildErrorPacketWithNonAsciiErrorMessage() {
        final ByteBuf buffer = Unpooled.buffer();
        final short errorCode = 1234;
        final String errorMessage = "Test error message with non-ASCII characters: 你好, 世界!";
        final byte sequenceId = 7;

        ErrPacketBuilder
                .create()
                .withErrorCode(errorCode)
                .withErrorMessage(errorMessage)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // Error packet header
        expectedPayloadLength += 2; // Error code
        expectedPayloadLength += errorMessage.getBytes(StandardCharsets.UTF_8).length; // Error message

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID

        // Verify packet content
        assertEquals(ErrPacketBuilder.ERR_PACKET_HEADER, buffer.readByte()); // Error packet header (0xFF)
        assertEquals(errorCode, buffer.readShortLE()); // Error code

        // Verify error message
        byte[] errorMessageBytes = new byte[errorMessage.getBytes(StandardCharsets.UTF_8).length];
        buffer.readBytes(errorMessageBytes);
        assertEquals(errorMessage, new String(errorMessageBytes, StandardCharsets.UTF_8));

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

}
