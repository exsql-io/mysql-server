package io.exsql.mysql.server.protocol.datatypes;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class LengthEncodedIntegerTest {

    @Test
    @DisplayName("Test encoding a byte value less than 251")
    void testEncodeByteValueLessThan251() {
        final ByteBuf buffer = Unpooled.buffer();
        final byte value = 123;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(1, buffer.readableBytes());
        assertEquals(value & 0xFF, buffer.readUnsignedByte());
    }

    @Test
    @DisplayName("Test encoding a short value less than 251")
    void testEncodeShortValueLessThan251() {
        final ByteBuf buffer = Unpooled.buffer();
        final short value = 250;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(1, buffer.readableBytes());
        assertEquals(value, buffer.readUnsignedByte());
    }

    @Test
    @DisplayName("Test encoding a short value between 251 and 65535")
    void testEncodeShortValueBetween251And65535() {
        final ByteBuf buffer = Unpooled.buffer();
        final short value = 1000;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(3, buffer.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_2_BYTES_PREFIX, buffer.readByte());
        assertEquals((byte) value, buffer.readByte());
        assertEquals((byte) (value >> 8), buffer.readByte());
    }

    @Test
    @DisplayName("Test encoding an int value less than 251")
    void testEncodeIntValueLessThan251() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 200;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(1, buffer.readableBytes());
        assertEquals(value, buffer.readUnsignedByte());
    }

    @Test
    @DisplayName("Test encoding an int value between 251 and 65535")
    void testEncodeIntValueBetween251And65535() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 60000;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(3, buffer.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_2_BYTES_PREFIX, buffer.readByte());
        assertEquals((byte) value, buffer.readByte());
        assertEquals((byte) (value >> 8), buffer.readByte());
    }

    @Test
    @DisplayName("Test encoding an int value between 65536 and 16777215")
    void testEncodeIntValueBetween65536And16777215() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 1000000;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(4, buffer.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_3_BYTES_PREFIX, buffer.readByte());
        assertEquals((byte) value, buffer.readByte());
        assertEquals((byte) (value >> 8), buffer.readByte());
        assertEquals((byte) (value >> 16), buffer.readByte());
    }

    @Test
    @DisplayName("Test encoding a long value less than 251")
    void testEncodeLongValueLessThan251() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = 200L;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(1, buffer.readableBytes());
        assertEquals(value, buffer.readUnsignedByte());
    }

    @Test
    @DisplayName("Test encoding a long value between 251 and 65535")
    void testEncodeLongValueBetween251And65535() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = 60000L;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(3, buffer.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_2_BYTES_PREFIX, buffer.readByte());
        assertEquals((byte) value, buffer.readByte());
        assertEquals((byte) (value >> 8), buffer.readByte());
    }

    @Test
    @DisplayName("Test encoding a long value between 65536 and 16777215")
    void testEncodeLongValueBetween65536And16777215() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = 1000000L;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(4, buffer.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_3_BYTES_PREFIX, buffer.readByte());
        assertEquals((byte) value, buffer.readByte());
        assertEquals((byte) (value >> 8), buffer.readByte());
        assertEquals((byte) (value >> 16), buffer.readByte());
    }

    @Test
    @DisplayName("Test encoding a long value between 16777216 and MAX_FIXED_LENGTH_INTEGER_8_BYTES_VALUE")
    void testEncodeLongValueBetween16777216AndMax8BytesValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = 1000000000L; // Value greater than MAX_FIXED_LENGTH_INTEGER_3_BYTES_VALUE

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(9, buffer.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_8_BYTES_PREFIX, buffer.readByte());
        assertEquals((byte) value, buffer.readByte());         // 1st byte (0 bits)
        assertEquals((byte) (value >> 8), buffer.readByte());  // 2nd byte (8 bits)
        assertEquals((byte) (value >> 16), buffer.readByte()); // 3rd byte (16 bits)
        assertEquals((byte) (value >> 24), buffer.readByte()); // 4th byte (24 bits)
        assertEquals((byte) (value >> 32), buffer.readByte()); // 5th byte (32 bits)
        assertEquals((byte) (value >> 40), buffer.readByte()); // 6th byte (40 bits)
        assertEquals((byte) (value >> 48), buffer.readByte()); // 7th byte (48 bits)
        assertEquals((byte) (value >> 56), buffer.readByte()); // 8th byte (56 bits)
    }

    @Test
    @DisplayName("Test encoding the maximum long value")
    void testEncodeMaxLongValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = Long.MAX_VALUE;

        LengthEncodedInteger.encode(buffer, value);
        assertEquals(9, buffer.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_8_BYTES_PREFIX, buffer.readByte());
        assertEquals((byte) value, buffer.readByte());         // 1st byte (0 bits)
        assertEquals((byte) (value >> 8), buffer.readByte());  // 2nd byte (8 bits)
        assertEquals((byte) (value >> 16), buffer.readByte()); // 3rd byte (16 bits)
        assertEquals((byte) (value >> 24), buffer.readByte()); // 4th byte (24 bits)
        assertEquals((byte) (value >> 32), buffer.readByte()); // 5th byte (32 bits)
        assertEquals((byte) (value >> 40), buffer.readByte()); // 6th byte (40 bits)
        assertEquals((byte) (value >> 48), buffer.readByte()); // 7th byte (48 bits)
        assertEquals((byte) (value >> 56), buffer.readByte()); // 8th byte (56 bits)
    }

    @Test
    @DisplayName("Test encoding at boundary values")
    void testEncodeBoundaryValues() {
        // Test at MAX_FIXED_LENGTH_INTEGER_1_BYTE_VALUE - 1
        final ByteBuf buffer1 = Unpooled.buffer();
        final long value1 = LengthEncodedInteger.MAX_FIXED_LENGTH_INTEGER_1_BYTE_VALUE - 1;

        LengthEncodedInteger.encode(buffer1, value1);
        assertEquals(1, buffer1.readableBytes());
        assertEquals(value1, buffer1.readUnsignedByte());

        // Test at MAX_FIXED_LENGTH_INTEGER_1_BYTE_VALUE
        final ByteBuf buffer2 = Unpooled.buffer();
        final long value2 = LengthEncodedInteger.MAX_FIXED_LENGTH_INTEGER_1_BYTE_VALUE;
        LengthEncodedInteger.encode(buffer2, value2);

        assertEquals(3, buffer2.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_2_BYTES_PREFIX, buffer2.readByte());
        assertEquals((byte) value2, buffer2.readByte());
        assertEquals((byte) (value2 >> 8), buffer2.readByte());

        // Test at MAX_FIXED_LENGTH_INTEGER_2_BYTES_VALUE - 1
        final ByteBuf buffer3 = Unpooled.buffer();
        final long value3 = LengthEncodedInteger.MAX_FIXED_LENGTH_INTEGER_2_BYTES_VALUE - 1;

        LengthEncodedInteger.encode(buffer3, value3);
        assertEquals(3, buffer3.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_2_BYTES_PREFIX, buffer3.readByte());
        assertEquals((byte) value3, buffer3.readByte());
        assertEquals((byte) (value3 >> 8), buffer3.readByte());

        // Test at MAX_FIXED_LENGTH_INTEGER_2_BYTES_VALUE
        final ByteBuf buffer4 = Unpooled.buffer();
        final long value4 = LengthEncodedInteger.MAX_FIXED_LENGTH_INTEGER_2_BYTES_VALUE;

        LengthEncodedInteger.encode(buffer4, value4);
        assertEquals(4, buffer4.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_3_BYTES_PREFIX, buffer4.readByte());
        assertEquals((byte) value4, buffer4.readByte());
        assertEquals((byte) (value4 >> 8), buffer4.readByte());
        assertEquals((byte) (value4 >> 16), buffer4.readByte());

        // Test at MAX_FIXED_LENGTH_INTEGER_3_BYTES_VALUE - 1
        final ByteBuf buffer5 = Unpooled.buffer();
        final long value5 = LengthEncodedInteger.MAX_FIXED_LENGTH_INTEGER_3_BYTES_VALUE - 1;

        LengthEncodedInteger.encode(buffer5, value5);
        assertEquals(4, buffer5.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_3_BYTES_PREFIX, buffer5.readByte());
        assertEquals((byte) value5, buffer5.readByte());
        assertEquals((byte) (value5 >> 8), buffer5.readByte());
        assertEquals((byte) (value5 >> 16), buffer5.readByte());

        // Test at MAX_FIXED_LENGTH_INTEGER_3_BYTES_VALUE
        final ByteBuf buffer6 = Unpooled.buffer();
        final long value6 = LengthEncodedInteger.MAX_FIXED_LENGTH_INTEGER_3_BYTES_VALUE;

        LengthEncodedInteger.encode(buffer6, value6);
        assertEquals(9, buffer6.readableBytes());
        assertEquals(LengthEncodedInteger.FIXED_LENGTH_INTEGER_8_BYTES_PREFIX, buffer6.readByte());
        assertEquals((byte) value6, buffer6.readByte());         // 1st byte (0 bits)
        assertEquals((byte) (value6 >> 8), buffer6.readByte());  // 2nd byte (8 bits)
        assertEquals((byte) (value6 >> 16), buffer6.readByte()); // 3rd byte (16 bits)
        assertEquals((byte) (value6 >> 24), buffer6.readByte()); // 4th byte (24 bits)
        assertEquals((byte) (value6 >> 32), buffer6.readByte()); // 5th byte (32 bits)
        assertEquals((byte) (value6 >> 40), buffer6.readByte()); // 6th byte (40 bits)
        assertEquals((byte) (value6 >> 48), buffer6.readByte()); // 7th byte (48 bits)
        assertEquals((byte) (value6 >> 56), buffer6.readByte()); // 8th byte (56 bits)
    }

    // Tests for decode functionality

    @Test
    @DisplayName("Test decoding a 1-byte value")
    void testDecode1ByteValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 123;

        buffer.writeByte(value);
        assertEquals(value, LengthEncodedInteger.decode(buffer));
    }

    @Test
    @DisplayName("Test decoding a 2-byte value")
    void testDecode2ByteValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 1000;

        buffer.writeByte(LengthEncodedInteger.FIXED_LENGTH_INTEGER_2_BYTES_PREFIX);
        buffer.writeByte((byte) value);
        buffer.writeByte((byte) (value >> 8));

        assertEquals(value, LengthEncodedInteger.decode(buffer));
    }

    @Test
    @DisplayName("Test decoding a 3-byte value")
    void testDecode3ByteValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final int value = 1000000;

        buffer.writeByte(LengthEncodedInteger.FIXED_LENGTH_INTEGER_3_BYTES_PREFIX);
        buffer.writeByte((byte) value);
        buffer.writeByte((byte) (value >> 8));
        buffer.writeByte((byte) (value >> 16));

        assertEquals(value, LengthEncodedInteger.decode(buffer));
    }

    @Test
    @DisplayName("Test decoding an 8-byte value")
    void testDecode8ByteValue() {
        final ByteBuf buffer = Unpooled.buffer();

        final long value = 1000000000L;
        buffer.writeByte(LengthEncodedInteger.FIXED_LENGTH_INTEGER_8_BYTES_PREFIX);
        buffer.writeLongLE(value);

        assertEquals(value, LengthEncodedInteger.decode(buffer));
    }

    @Test
    @DisplayName("Test decoding the maximum long value")
    void testDecodeMaxLongValue() {
        final ByteBuf buffer = Unpooled.buffer();
        final long value = Long.MAX_VALUE;

        buffer.writeByte(LengthEncodedInteger.FIXED_LENGTH_INTEGER_8_BYTES_PREFIX);
        buffer.writeLongLE(value);

        assertEquals(value, LengthEncodedInteger.decode(buffer));
    }

    @Test
    @DisplayName("Test decoding with invalid prefix")
    void testDecodeInvalidPrefix() {
        final ByteBuf buffer = Unpooled.buffer();
        buffer.writeByte(0xFF); // Invalid prefix

        assertThrows(IllegalArgumentException.class, () -> LengthEncodedInteger.decode(buffer));
    }

    @Test
    @DisplayName("Test encode-decode roundtrip")
    void testEncodeDecodeRoundtrip() {
        // Test with small value
        ByteBuf buffer = Unpooled.buffer();
        long value = 123;
        LengthEncodedInteger.encode(buffer, value);
        buffer.readerIndex(0);
        assertEquals(value, LengthEncodedInteger.decode(buffer));

        // Test with medium value
        buffer = Unpooled.buffer();
        value = 60000;
        LengthEncodedInteger.encode(buffer, value);
        buffer.readerIndex(0);
        assertEquals(value, LengthEncodedInteger.decode(buffer));

        // Test with large value
        buffer = Unpooled.buffer();
        value = 1000000;
        LengthEncodedInteger.encode(buffer, value);
        buffer.readerIndex(0);
        assertEquals(value, LengthEncodedInteger.decode(buffer));

        // Test with very large value
        buffer = Unpooled.buffer();
        value = 1000000000L;
        LengthEncodedInteger.encode(buffer, value);
        buffer.readerIndex(0);
        assertEquals(value, LengthEncodedInteger.decode(buffer));
    }
}
