package io.exsql.mysql.server.protocol.datatypes;

import io.netty.buffer.ByteBuf;

/**
 * Implementation of MySQL's length-encoded integer type.
 * In the MySQL wire protocol, length-encoded integers are used to represent values
 * with a variable number of bytes depending on the value being encoded:
 * - Values < 251: encoded as a single byte
 * - Values between 251 and 65535: encoded with 0xFC prefix + 2 bytes
 * - Values between 65536 and 16777215: encoded with 0xFD prefix + 3 bytes
 * - Values >= 16777216: encoded with 0xFE prefix + 8 bytes
 * Unlike fixed-length integers, length-encoded integers use a variable number
 * of bytes based on the magnitude of the value being encoded.
 */
public final class LengthEncodedInteger {

    public final static short MAX_FIXED_LENGTH_INTEGER_1_BYTE_VALUE = 251;

    public final static int MAX_FIXED_LENGTH_INTEGER_2_BYTES_VALUE = 65536;

    public final static int MAX_FIXED_LENGTH_INTEGER_3_BYTES_VALUE = 16777216;

    public final static byte FIXED_LENGTH_INTEGER_2_BYTES_PREFIX = (byte) 0xFC;

    public final static byte FIXED_LENGTH_INTEGER_3_BYTES_PREFIX = (byte) 0xFD;

    public final static byte FIXED_LENGTH_INTEGER_8_BYTES_PREFIX = (byte) 0xFE;

    private LengthEncodedInteger() {}

    /**
     * Encodes a long value as a length-encoded integer.
     *
     * @param buffer The buffer to write to
     * @param value The long value to encode
     */
    public static int encode(final ByteBuf buffer, final long value) {
        if (value < MAX_FIXED_LENGTH_INTEGER_1_BYTE_VALUE) {
            return FixedLengthInteger.encode1(buffer, (int) value);
        } else if (value < MAX_FIXED_LENGTH_INTEGER_2_BYTES_VALUE) {
            buffer.writeByte(FIXED_LENGTH_INTEGER_2_BYTES_PREFIX);
            return FixedLengthInteger.encode2(buffer, (int) value) + 1;
        } else if (value < MAX_FIXED_LENGTH_INTEGER_3_BYTES_VALUE) {
            buffer.writeByte(FIXED_LENGTH_INTEGER_3_BYTES_PREFIX);
            return FixedLengthInteger.encode3(buffer, (int) value) + 1;
        } else {
            // Since MAX_FIXED_LENGTH_INTEGER_8_BYTES_VALUE is -1 (due to unsigned long representation),
            // we can't use a direct comparison. Instead, we'll handle all values >= MAX_FIXED_LENGTH_INTEGER_3_BYTES_VALUE
            // as 8-byte integers, as long as they're not too large.
            buffer.writeByte(FIXED_LENGTH_INTEGER_8_BYTES_PREFIX);
            return FixedLengthInteger.encode8(buffer, value) + 1;
        }
    }

    /**
     * Decodes a length-encoded integer from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded value
     */
    public static long decode(final ByteBuf buffer) {
        final short firstByte = buffer.readUnsignedByte();

        if (firstByte < MAX_FIXED_LENGTH_INTEGER_1_BYTE_VALUE) {
            return firstByte;
        } else if (firstByte == (FIXED_LENGTH_INTEGER_2_BYTES_PREFIX & 0xFF)) {
            return FixedLengthInteger.decode2(buffer);
        } else if (firstByte == (FIXED_LENGTH_INTEGER_3_BYTES_PREFIX & 0xFF)) {
            return FixedLengthInteger.decode3(buffer);
        } else if (firstByte == (FIXED_LENGTH_INTEGER_8_BYTES_PREFIX & 0xFF) && buffer.readableBytes() >= 8) {
            return FixedLengthInteger.decode8(buffer);
        } else {
            // This should not happen with valid MySQL protocol data
            throw new IllegalArgumentException("Invalid length-encoded integer prefix: " + firstByte);
        }
    }

}
