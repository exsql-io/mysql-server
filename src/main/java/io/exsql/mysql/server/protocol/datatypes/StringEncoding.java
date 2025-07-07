package io.exsql.mysql.server.protocol.datatypes;

import io.netty.buffer.ByteBuf;
import reactor.util.annotation.NonNull;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * Implementation of MySQL's string data types.
 * In the MySQL wire protocol, strings can be encoded in several ways:
 * 1. Fixed-length strings: A string with a predetermined length.
 * 2. Null-terminated strings: A string terminated by a null byte (0x00).
 * 3. Length-encoded strings: A string preceded by a length-encoded integer that specifies the length of the string.
 * 4. Variable-length strings: A string that consumes all remaining bytes in the buffer.
 */
public final class StringEncoding {

    private static final byte NULL_TERMINATOR = 0x00;

    private StringEncoding() {}

    /**
     * Encodes a byte array as a fixed-length string.
     *
     * @param buffer The buffer to write to
     * @param bytes The byte array to encode
     * @param length The fixed length of the string
     */
    public static int encodeFixedLength(final ByteBuf buffer, final byte[] bytes, final int length) {
        if (bytes.length > length) {
            // If the byte array is too long, truncate it
            buffer.writeBytes(bytes, 0, length);
        } else {
            // If the byte array is shorter than the fixed length, pad with zeros
            buffer.writeBytes(bytes);
            for (int i = bytes.length; i < length; i++) {
                buffer.writeByte(0);
            }
        }

        return length;
    }

    /**
     * Encodes a string as a fixed-length string.
     *
     * @param buffer The buffer to write to
     * @param value The string to encode
     * @param length The fixed length of the string
     * @param charset The character set to use for encoding
     */
    public static void encodeFixedLength(final ByteBuf buffer, final String value, final int length, @NonNull final Charset charset) {
        final byte[] bytes = value.getBytes(charset);
        encodeFixedLength(buffer, bytes, length);
    }

    /**
     * Encodes a string as a fixed-length string using UTF-8 encoding.
     *
     * @param buffer The buffer to write to
     * @param value The string to encode
     * @param length The fixed length of the string
     */
    public static void encodeFixedLength(final ByteBuf buffer, final String value, final int length) {
        encodeFixedLength(buffer, value, length, StandardCharsets.UTF_8);
    }

    /**
     * Decodes a fixed-length byte array from the buffer.
     *
     * @param buffer The buffer to read from
     * @param length The fixed length of the byte array
     * @return The decoded byte array with trailing zeros trimmed
     */
    public static byte[] decodeFixedLengthBytes(final ByteBuf buffer, final int length) {
        final byte[] bytes = new byte[length];
        buffer.readBytes(bytes);

        // Trim trailing zeros
        int actualLength = length;
        while (actualLength > 0 && bytes[actualLength - 1] == 0) {
            actualLength--;
        }

        if (actualLength == length) {
            return bytes;
        } else {
            final byte[] result = new byte[actualLength];
            System.arraycopy(bytes, 0, result, 0, actualLength);
            return result;
        }
    }

    /**
     * Decodes a fixed-length string from the buffer.
     *
     * @param buffer The buffer to read from
     * @param length The fixed length of the string
     * @param charset The character set to use for decoding
     * @return The decoded string
     */
    public static String decodeFixedLength(final ByteBuf buffer, final int length, @NonNull final Charset charset) {
        final byte[] bytes = decodeFixedLengthBytes(buffer, length);
        return new String(bytes, charset);
    }

    /**
     * Decodes a fixed-length string from the buffer using UTF-8 encoding.
     *
     * @param buffer The buffer to read from
     * @param length The fixed length of the string
     * @return The decoded string
     */
    public static String decodeFixedLength(final ByteBuf buffer, final int length) {
        return decodeFixedLength(buffer, length, StandardCharsets.UTF_8);
    }

    /**
     * Encodes a byte array as a null-terminated string.
     *
     * @param buffer The buffer to write to
     * @param bytes The byte array to encode
     */
    public static int encodeNullTerminated(final ByteBuf buffer, final byte[] bytes) {
        buffer.writeBytes(bytes);
        buffer.writeByte(NULL_TERMINATOR);
        return bytes.length + 1;
    }

    /**
     * Encodes a string as a null-terminated string.
     *
     * @param buffer The buffer to write to
     * @param value The string to encode
     * @param charset The character set to use for encoding
     */
    public static int encodeNullTerminated(final ByteBuf buffer, final String value, @NonNull final Charset charset) {
        return encodeNullTerminated(buffer, value.getBytes(charset));
    }

    /**
     * Encodes a string as a null-terminated string using UTF-8 encoding.
     *
     * @param buffer The buffer to write to
     * @param value The string to encode
     */
    public static int encodeNullTerminated(final ByteBuf buffer, final String value) {
        return encodeNullTerminated(buffer, value, StandardCharsets.UTF_8);
    }

    /**
     * Decodes a null-terminated byte array from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded byte array (without the null terminator)
     */
    public static byte[] decodeNullTerminatedBytes(final ByteBuf buffer) {
        final int startIndex = buffer.readerIndex();
        int length = 0;

        // Find the null terminator
        while (buffer.readByte() != NULL_TERMINATOR) {
            length++;

            if (!buffer.isReadable()) {
                throw new IllegalArgumentException("No null terminator found in the buffer");
            }
        }

        // Reset reader index to read the bytes
        buffer.readerIndex(startIndex);
        final byte[] bytes = new byte[length];
        buffer.readBytes(bytes);

        // Skip the null terminator
        buffer.skipBytes(1);

        return bytes;
    }

    /**
     * Decodes a null-terminated string from the buffer.
     *
     * @param buffer The buffer to read from
     * @param charset The character set to use for decoding
     * @return The decoded string
     */
    public static String decodeNullTerminated(final ByteBuf buffer, @NonNull final Charset charset) {
        final byte[] bytes = decodeNullTerminatedBytes(buffer);
        return new String(bytes, charset);
    }

    /**
     * Decodes a null-terminated string from the buffer using UTF-8 encoding.
     *
     * @param buffer The buffer to read from
     * @return The decoded string
     */
    public static String decodeNullTerminated(final ByteBuf buffer) {
        return decodeNullTerminated(buffer, StandardCharsets.UTF_8);
    }

    /**
     * Encodes a byte array as a length-encoded string.
     *
     * @param buffer The buffer to write to
     * @param bytes The byte array to encode
     */
    public static int encodeLengthEncoded(final ByteBuf buffer, final byte[] bytes) {
        // Write the length of the byte array as a length-encoded integer
        var length = LengthEncodedInteger.encode(buffer, bytes.length);

        // Write the bytes
        buffer.writeBytes(bytes);

        length += bytes.length;
        return length;
    }

    /**
     * Encodes a string as a length-encoded string.
     *
     * @param buffer The buffer to write to
     * @param value The string to encode
     * @param charset The character set to use for encoding
     */
    public static int encodeLengthEncoded(final ByteBuf buffer, final String value, @NonNull final Charset charset) {
        return encodeLengthEncoded(buffer, value.getBytes(charset));
    }

    /**
     * Encodes a string as a length-encoded string using UTF-8 encoding.
     *
     * @param buffer The buffer to write to
     * @param value The string to encode
     */
    public static int encodeLengthEncoded(final ByteBuf buffer, final String value) {
        return encodeLengthEncoded(buffer, value, StandardCharsets.UTF_8);
    }

    /**
     * Decodes a length-encoded byte array from the buffer.
     *
     * @param buffer The buffer to read from
     * @return The decoded byte array
     */
    public static byte[] decodeLengthEncodedBytes(final ByteBuf buffer) {
        // Read the length of the byte array as a length-encoded integer
        final long length = LengthEncodedInteger.decode(buffer);
        if (length > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Byte array length exceeds maximum allowed: " + length);
        }

        // Read the bytes
        final byte[] bytes = new byte[(int) length];
        buffer.readBytes(bytes);

        return bytes;
    }

    /**
     * Decodes a length-encoded string from the buffer.
     *
     * @param buffer The buffer to read from
     * @param charset The character set to use for decoding
     * @return The decoded string
     */
    public static String decodeLengthEncoded(final ByteBuf buffer, @NonNull final Charset charset) {
        final byte[] bytes = decodeLengthEncodedBytes(buffer);
        return new String(bytes, charset);
    }

    /**
     * Decodes a length-encoded string from the buffer using UTF-8 encoding.
     *
     * @param buffer The buffer to read from
     * @return The decoded string
     */
    public static String decodeLengthEncoded(final ByteBuf buffer) {
        return decodeLengthEncoded(buffer, StandardCharsets.UTF_8);
    }

}
