package io.exsql.mysql.server.protocol.datatypes;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FixedLengthIntegerTest {

    @Test
    @DisplayName("Test encoding and decoding a 1-byte integer")
    void testEncodeDecode1ByteInteger() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 123;

        FixedLengthInteger.encode1(buffer, value);
        assertEquals(1, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode1(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 1-byte integer at max value")
    void testEncodeDecode1ByteIntegerMaxValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 0xFF; // Max value for 1 byte

        FixedLengthInteger.encode1(buffer, value);
        assertEquals(1, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode1(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 2-byte integer")
    void testEncodeDecode2ByteInteger() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 12345;

        FixedLengthInteger.encode2(buffer, value);
        assertEquals(2, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode2(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 2-byte integer at max value")
    void testEncodeDecode2ByteIntegerMaxValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 0xFFFF; // Max value for 2 bytes

        FixedLengthInteger.encode2(buffer, value);
        assertEquals(2, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode2(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 3-byte integer")
    void testEncodeDecode3ByteInteger() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 1234567;

        FixedLengthInteger.encode3(buffer, value);
        assertEquals(3, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode3(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 3-byte integer at max value")
    void testEncodeDecode3ByteIntegerMaxValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 0xFFFFFF; // Max value for 3 bytes

        FixedLengthInteger.encode3(buffer, value);
        assertEquals(3, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode3(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 4-byte integer")
    void testEncodeDecode4ByteInteger() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 123456789;

        FixedLengthInteger.encode4(buffer, value);
        assertEquals(4, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode4(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 4-byte integer at max value")
    void testEncodeDecode4ByteIntegerMaxValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 0x7FFFFFFF; // Max positive value for 4 bytes (signed int)

        FixedLengthInteger.encode4(buffer, value);
        assertEquals(4, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode4(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 6-byte integer")
    void testEncodeDecode6ByteInteger() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = 123456789012L;

        FixedLengthInteger.encode6(buffer, value);
        assertEquals(6, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode6(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding a 6-byte integer at max value")
    void testEncodeDecode6ByteIntegerMaxValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = 0xFFFFFFFFFFL; // Max value for 6 bytes

        FixedLengthInteger.encode6(buffer, value);
        assertEquals(6, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode6(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding an 8-byte integer")
    void testEncodeDecode8ByteInteger() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = 1234567890123456789L;

        FixedLengthInteger.encode8(buffer, value);
        assertEquals(8, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode8(buffer));
    }

    @Test
    @DisplayName("Test encoding and decoding an 8-byte integer at max value")
    void testEncodeDecode8ByteIntegerMaxValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = Long.MAX_VALUE;

        FixedLengthInteger.encode8(buffer, value);
        assertEquals(8, buffer.readableBytes());
        assertEquals(value, FixedLengthInteger.decode8(buffer));
    }

    @Test
    @DisplayName("Test byte order for 3-byte integer")
    void testByteOrderFor3ByteInteger() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 0x123456; // A value with distinct bytes

        FixedLengthInteger.encode3(buffer, value);
        assertEquals(3, buffer.readableBytes());
        
        // In little-endian format, least significant byte comes first
        assertEquals(0x56, buffer.getByte(0) & 0xFF);
        assertEquals(0x34, buffer.getByte(1) & 0xFF);
        assertEquals(0x12, buffer.getByte(2) & 0xFF);
        
        // Reset reader index to decode
        buffer.readerIndex(0);
        assertEquals(value, FixedLengthInteger.decode3(buffer));
    }

    @Test
    @DisplayName("Test byte order for 6-byte integer")
    void testByteOrderFor6ByteInteger() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = 0x123456789ABCL; // A value with distinct bytes

        FixedLengthInteger.encode6(buffer, value);
        assertEquals(6, buffer.readableBytes());
        
        // In little-endian format, least significant byte comes first
        assertEquals(0xBC, buffer.getByte(0) & 0xFF);
        assertEquals(0x9A, buffer.getByte(1) & 0xFF);
        assertEquals(0x78, buffer.getByte(2) & 0xFF);
        assertEquals(0x56, buffer.getByte(3) & 0xFF);
        assertEquals(0x34, buffer.getByte(4) & 0xFF);
        assertEquals(0x12, buffer.getByte(5) & 0xFF);
        
        // Reset reader index to decode
        buffer.readerIndex(0);
        assertEquals(value, FixedLengthInteger.decode6(buffer));
    }
}