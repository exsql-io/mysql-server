package io.exsql.mysql.server.protocol.datatypes;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

public class StringEncodingTest {

    @Test
    @DisplayName("Test encoding and decoding a fixed-length string")
    void testEncodeDecodeFixedLengthString() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello";
        final int length = 10;

        StringEncoding.encodeFixedLength(buffer, value, length);
        assertEquals(length, buffer.readableBytes());
        assertEquals(value, StringEncoding.decodeFixedLength(buffer, length));
    }

    @Test
    @DisplayName("Test encoding and decoding a fixed-length string with exact length")
    void testEncodeDecodeFixedLengthStringExactLength() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello";
        final int length = 5;

        StringEncoding.encodeFixedLength(buffer, value, length);
        assertEquals(length, buffer.readableBytes());
        assertEquals(value, StringEncoding.decodeFixedLength(buffer, length));
    }

    @Test
    @DisplayName("Test encoding and decoding a fixed-length string that is too long")
    void testEncodeDecodeFixedLengthStringTooLong() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "HelloWorld";
        final int length = 5;

        StringEncoding.encodeFixedLength(buffer, value, length);
        assertEquals(length, buffer.readableBytes());
        assertEquals("Hello", StringEncoding.decodeFixedLength(buffer, length));
    }

    @Test
    @DisplayName("Test encoding and decoding a fixed-length string with custom charset")
    void testEncodeDecodeFixedLengthStringWithCharset() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello";
        final int length = 10;

        StringEncoding.encodeFixedLength(buffer, value, length, StandardCharsets.ISO_8859_1);
        assertEquals(length, buffer.readableBytes());
        assertEquals(value, StringEncoding.decodeFixedLength(buffer, length, StandardCharsets.ISO_8859_1));
    }

    @Test
    @DisplayName("Test encoding and decoding a null-terminated string")
    void testEncodeDecodeNullTerminatedString() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello";

        var bytes = StringEncoding.encodeNullTerminated(buffer, value);
        assertEquals(value.length() + 1, buffer.readableBytes()); // +1 for null terminator
        assertEquals(value.length() + 1, bytes);
        assertEquals(value, StringEncoding.decodeNullTerminated(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding an empty null-terminated string")
    void testEncodeDecodeEmptyNullTerminatedString() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "";

        var bytes = StringEncoding.encodeNullTerminated(buffer, value);
        assertEquals(1, buffer.readableBytes()); // Just the null terminator
        assertEquals(1, bytes);
        assertEquals(value, StringEncoding.decodeNullTerminated(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a null-terminated string with custom charset")
    void testEncodeDecodeNullTerminatedStringWithCharset() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello";

        var bytes = StringEncoding.encodeNullTerminated(buffer, value, StandardCharsets.ISO_8859_1);
        assertEquals(value.length() + 1, buffer.readableBytes()); // +1 for null terminator
        assertEquals(value.length() + 1, bytes);
        assertEquals(value, StringEncoding.decodeNullTerminated(buffer, StandardCharsets.ISO_8859_1));
    }

    @Test
    @DisplayName("Test decoding a null-terminated string with no null terminator")
    void testDecodeNullTerminatedStringNoTerminator() {
        final ByteBuf buffer = Unpooled.buffer();
        buffer.writeBytes("Hello".getBytes(StandardCharsets.UTF_8));

        assertThrows(IllegalArgumentException.class, () -> StringEncoding.decodeNullTerminated(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a length-encoded string")
    void testEncodeDecodeLengthEncodedString() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello";

        StringEncoding.encodeLengthEncoded(buffer, value);
        // 1 byte for length (since length is < 251) + 5 bytes for "Hello"
        assertEquals(1 + value.length(), buffer.readableBytes());
        assertEquals(value, StringEncoding.decodeLengthEncoded(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding an empty length-encoded string")
    void testEncodeDecodeEmptyLengthEncodedString() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "";

        StringEncoding.encodeLengthEncoded(buffer, value);
        assertEquals(1, buffer.readableBytes()); // Just the length (0)
        assertEquals(value, StringEncoding.decodeLengthEncoded(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a long length-encoded string")
    void testEncodeDecodeLongLengthEncodedString() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "a".repeat(1000);

        StringEncoding.encodeLengthEncoded(buffer, value);
        // 3 bytes for length (since length is > 251 but < 65536) + 1000 bytes for the string
        assertEquals(3 + value.length(), buffer.readableBytes());
        assertEquals(value, StringEncoding.decodeLengthEncoded(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a length-encoded string with custom charset")
    void testEncodeDecodeLengthEncodedStringWithCharset() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello";

        StringEncoding.encodeLengthEncoded(buffer, value, StandardCharsets.ISO_8859_1);
        assertEquals(1 + value.length(), buffer.readableBytes());
        assertEquals(value, StringEncoding.decodeLengthEncoded(buffer, StandardCharsets.ISO_8859_1));
    }

    @Test
    @DisplayName("Test roundtrip encoding and decoding for all string types")
    void testRoundtripEncodingDecoding() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello, World!";

        // Fixed-length
        StringEncoding.encodeFixedLength(buffer, value, 20);
        buffer.markReaderIndex();
        assertEquals(value, StringEncoding.decodeFixedLength(buffer, 20));
        buffer.resetReaderIndex();
        buffer.clear();

        // Null-terminated
        StringEncoding.encodeNullTerminated(buffer, value);
        buffer.markReaderIndex();
        assertEquals(value, StringEncoding.decodeNullTerminated(buffer));
        buffer.resetReaderIndex();
        buffer.clear();

        // Length-encoded
        StringEncoding.encodeLengthEncoded(buffer, value);
        buffer.markReaderIndex();
        assertEquals(value, StringEncoding.decodeLengthEncoded(buffer));
        buffer.resetReaderIndex();
        buffer.clear();
    }

    @Test
    @DisplayName("Test encoding and decoding strings with special characters")
    void testEncodeDecodeSpecialCharacters() {
        final ByteBuf buffer = Unpooled.buffer();
        final String value = "Hello, 世界!";

        // Fixed-length
        StringEncoding.encodeFixedLength(buffer, value, 20);
        assertEquals(value, StringEncoding.decodeFixedLength(buffer, 20));
        buffer.clear();

        // Null-terminated
        StringEncoding.encodeNullTerminated(buffer, value);
        assertEquals(value, StringEncoding.decodeNullTerminated(buffer));
        buffer.clear();

        // Length-encoded
        StringEncoding.encodeLengthEncoded(buffer, value);
        assertEquals(value, StringEncoding.decodeLengthEncoded(buffer));
        buffer.clear();
    }

    @Test
    @DisplayName("Test encoding and decoding byte arrays with fixed length")
    void testEncodeDecodeFixedLengthBytes() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte[] value = {1, 2, 3, 4, 5};
        final int length = 10;

        StringEncoding.encodeFixedLength(buffer, value, length);
        assertEquals(length, buffer.readableBytes());

        final byte[] decoded = StringEncoding.decodeFixedLengthBytes(buffer, length);
        assertEquals(value.length, decoded.length);
        assertArrayEquals(value, decoded);
    }

    @Test
    @DisplayName("Test encoding and decoding byte arrays with fixed length that is too long")
    void testEncodeDecodeFixedLengthBytesTooLong() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte[] value = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        final int length = 5;

        StringEncoding.encodeFixedLength(buffer, value, length);
        assertEquals(length, buffer.readableBytes());

        final byte[] decoded = StringEncoding.decodeFixedLengthBytes(buffer, length);
        assertEquals(length, decoded.length);
        for (int i = 0; i < length; i++) {
            assertEquals(value[i], decoded[i]);
        }
    }

    @Test
    @DisplayName("Test encoding and decoding null-terminated byte arrays")
    void testEncodeDecodeNullTerminatedBytes() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte[] value = {1, 2, 3, 4, 5};

        StringEncoding.encodeNullTerminated(buffer, value);
        assertEquals(value.length + 1, buffer.readableBytes()); // +1 for null terminator

        final byte[] decoded = StringEncoding.decodeNullTerminatedBytes(buffer);
        assertEquals(value.length, decoded.length);
        assertArrayEquals(value, decoded);
    }

    @Test
    @DisplayName("Test encoding and decoding empty null-terminated byte arrays")
    void testEncodeDecodeEmptyNullTerminatedBytes() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte[] value = new byte[0];

        StringEncoding.encodeNullTerminated(buffer, value);
        assertEquals(1, buffer.readableBytes()); // Just the null terminator

        final byte[] decoded = StringEncoding.decodeNullTerminatedBytes(buffer);
        assertEquals(0, decoded.length);
    }

    @Test
    @DisplayName("Test encoding and decoding length-encoded byte arrays")
    void testEncodeDecodeLengthEncodedBytes() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte[] value = {1, 2, 3, 4, 5};

        StringEncoding.encodeLengthEncoded(buffer, value);
        // 1 byte for length (since length is < 251) + 5 bytes for the data
        assertEquals(1 + value.length, buffer.readableBytes());

        final byte[] decoded = StringEncoding.decodeLengthEncodedBytes(buffer);
        assertEquals(value.length, decoded.length);
        assertArrayEquals(value, decoded);
    }

    @Test
    @DisplayName("Test encoding and decoding empty length-encoded byte arrays")
    void testEncodeDecodeEmptyLengthEncodedBytes() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte[] value = new byte[0];

        StringEncoding.encodeLengthEncoded(buffer, value);
        assertEquals(1, buffer.readableBytes()); // Just the length (0)

        final byte[] decoded = StringEncoding.decodeLengthEncodedBytes(buffer);
        assertEquals(0, decoded.length);
    }

    @Test
    @DisplayName("Test roundtrip encoding and decoding for all byte array types")
    void testRoundtripEncodingDecodingBytes() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte[] value = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};

        // Fixed-length
        StringEncoding.encodeFixedLength(buffer, value, 15);
        buffer.markReaderIndex();
        final byte[] fixedDecoded = StringEncoding.decodeFixedLengthBytes(buffer, 15);
        assertArrayEquals(value, fixedDecoded);
        buffer.resetReaderIndex();
        buffer.clear();

        // Null-terminated
        StringEncoding.encodeNullTerminated(buffer, value);
        buffer.markReaderIndex();
        final byte[] nullTerminatedDecoded = StringEncoding.decodeNullTerminatedBytes(buffer);
        assertArrayEquals(value, nullTerminatedDecoded);
        buffer.resetReaderIndex();
        buffer.clear();

        // Length-encoded
        StringEncoding.encodeLengthEncoded(buffer, value);
        buffer.markReaderIndex();
        final byte[] lengthEncodedDecoded = StringEncoding.decodeLengthEncodedBytes(buffer);
        assertArrayEquals(value, lengthEncodedDecoded);
        buffer.resetReaderIndex();
        buffer.clear();
    }

}
