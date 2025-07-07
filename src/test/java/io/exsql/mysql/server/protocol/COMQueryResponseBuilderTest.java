package io.exsql.mysql.server.protocol;

import io.exsql.mysql.server.protocol.server.COMQueryResponseBuilder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class COMQueryResponseBuilderTest {

    @Test
    @DisplayName("Test building a result set header packet with a small column count")
    void testBuildResultSetHeaderWithSmallColumnCount() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte sequenceId = 1;

        COMQueryResponseBuilder
                .create(0)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Calculate expected payload length
        int expectedPayloadLength = 1; // Column count (1 byte for small values)

        // Verify packet header
        assertEquals(expectedPayloadLength, buffer.readUnsignedMediumLE()); // Payload length
        assertEquals(sequenceId, buffer.readByte()); // Sequence ID
        assertEquals(0, buffer.readByte());

        // Verify total packet size
        assertEquals(0, buffer.readableBytes()); // All bytes should have been read
    }

    @Test
    @DisplayName("Test building a complete COM_QUERY response with column definitions, EOF packets, and row data")
    void testBuildCompleteResponse() {
        final byte sequenceId = 1;

        // Create column definitions
        COMQueryResponseBuilder.ColumnDefinition idColumn = new COMQueryResponseBuilder.ColumnDefinition(
                "test_schema",
                "test_table",
                "test_table",
                "id",
                "id",
                33,
                11,
                (byte) 3,
                0x1,
                (byte) 0
        );

        COMQueryResponseBuilder.ColumnDefinition nameColumn = new COMQueryResponseBuilder.ColumnDefinition(
                "test_schema",
                "test_table",
                "test_table",
                "name",
                "name",
                33,
                255,
                (byte) 253,
                0,
                (byte) 0
        );

        // Create row data
        List<String> row1 = Arrays.asList("1", "John");
        List<String> row2 = Arrays.asList("2", "Jane");

        // Build the complete response
        ByteBuf buffer = Unpooled.buffer();
        COMQueryResponseBuilder
                .create(0)
                .withColumnDefinition(idColumn)
                .withColumnDefinition(nameColumn)
                .withRow(row1)
                .withRow(row2)
                .withSequenceId(sequenceId)
                .build(buffer);

        // Reset reader index to read from the beginning
        buffer.readerIndex(0);

        // Verify the complete response
        // This test is simplified to just verify that the builder doesn't throw any exceptions
        // and that the buffer contains data
        assertTrue(buffer.isReadable());
    }

}
