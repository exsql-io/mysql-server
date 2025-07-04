package io.exsql.mysql.server.protocol.datatypes;

import io.netty.buffer.ByteBuf;

/**
 * Implementation of MySQL's fixed-length integer type.
 * In the MySQL wire protocol, fixed-length integers are used to represent values
 * with a predetermined size (1, 2, 3, 4, 6, or 8 bytes).
 * Unlike length-encoded integers, fixed-length integers always use the same number
 * of bytes regardless of the value being encoded.
 */
public final class FixedLengthInteger {

    private FixedLengthInteger() {}

    /**
     * Encodes a value as a 1-byte integer.
     *
     * @param buffer The buffer to write to
     * @param value The value to encode
     */
    public static int encode1(final ByteBuf buffer, final int value) {
        buffer.writeByte(value);
        return 1;
    }

    /**
     * Encodes a value as a 2-byte integer.
     *
     * @param buffer The buffer to write to
     * @param value The value to encode
     */
    public static int encode2(final ByteBuf buffer, final int value) {
        buffer.writeShortLE(value);
        return 2;
    }

    /**
     * Encodes a value as a 3-byte integer.
     *
     * @param buffer The buffer to write to
     * @param value The value to encode
     */
    public static int encode3(final ByteBuf buffer, final int value) {
        buffer.writeByte(value);
        buffer.writeByte(value >> IntegerEncoding.SECOND_BYTE_OFFSET);
        buffer.writeByte(value >> IntegerEncoding.THIRD_BYTE_OFFSET);
        return 3;
    }

    /**
     * Encodes a value as a 4-byte integer.
     *
     * @param buffer The buffer to write to
     * @param value The value to encode
     */
    public static int encode4(final ByteBuf buffer, final int value) {
        buffer.writeIntLE(value);
        return 4;
    }

    /**
     * Encodes a value as a 6-byte integer.
     *
     * @param buffer The buffer to write to
     * @param value The value to encode
     */
    public static int encode6(final ByteBuf buffer, final long value) {
        buffer.writeByte((byte) value);
        buffer.writeByte((byte) (value >> IntegerEncoding.SECOND_BYTE_OFFSET));
        buffer.writeByte((byte) (value >> IntegerEncoding.THIRD_BYTE_OFFSET));
        buffer.writeByte((byte) (value >> IntegerEncoding.FOURTH_BYTE_OFFSET));
        buffer.writeByte((byte) (value >> IntegerEncoding.FIFTH_BYTE_OFFSET));
        buffer.writeByte((byte) (value >> IntegerEncoding.SIXTH_BYTE_OFFSET));
        return 6;
    }

    /**
     * Encodes a value as an 8-byte integer.
     *
     * @param buffer The buffer to write to
     * @param value The value to encode
     */
    public static int encode8(final ByteBuf buffer, final long value) {
        buffer.writeLongLE(value);
        return 8;
    }

    /**
     * Decodes a 1-byte integer from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded value
     */
    public static short decode1(final ByteBuf buffer) {
        return buffer.readUnsignedByte();
    }

    /**
     * Decodes a 2-byte integer from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded value
     */
    public static int decode2(final ByteBuf buffer) {
        return buffer.readUnsignedShortLE();
    }

    /**
     * Decodes a 3-byte integer from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded value
     */
    public static int decode3(final ByteBuf buffer) {
        int value = buffer.readUnsignedByte();
        value |= buffer.readUnsignedByte() << IntegerEncoding.SECOND_BYTE_OFFSET;
        value |= buffer.readUnsignedByte() << IntegerEncoding.THIRD_BYTE_OFFSET;
        return value;
    }

    /**
     * Decodes a 4-byte integer from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded value
     */
    public static int decode4(final ByteBuf buffer) {
        return buffer.readIntLE();
    }

    /**
     * Decodes a 6-byte integer from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded value
     */
    public static long decode6(final ByteBuf buffer) {
        long value = buffer.readUnsignedByte();
        value |= (long) buffer.readUnsignedByte() << IntegerEncoding.SECOND_BYTE_OFFSET;
        value |= (long) buffer.readUnsignedByte() << IntegerEncoding.THIRD_BYTE_OFFSET;
        value |= (long) buffer.readUnsignedByte() << IntegerEncoding.FOURTH_BYTE_OFFSET;
        value |= (long) buffer.readUnsignedByte() << IntegerEncoding.FIFTH_BYTE_OFFSET;
        value |= (long) buffer.readUnsignedByte() << IntegerEncoding.SIXTH_BYTE_OFFSET;
        return value;
    }

    /**
     * Decodes an 8-byte integer from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded value
     */
    public static long decode8(final ByteBuf buffer) {
        return buffer.readLongLE();
    }

}