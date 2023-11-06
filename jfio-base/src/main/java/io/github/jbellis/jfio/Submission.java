package io.github.jbellis.jfio;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A read operation to be submitted to an {@link IORing}.
 */
public abstract class Submission {
    private final int fd;
    private final int length;
    private final ByteBuffer buffer;
    private final long offset;

    /**
     * Creates a new submission.
     *
     * @param fd the file descriptor of the file to read from.
     * @param length the number of bytes to read.
     * @param buffer the buffer to read into. This <b>must</b> be a direct buffer.
     * @param offset the offset in the file at which to read.
     */
    protected Submission(int fd, int length, ByteBuffer buffer, long offset) {
        Objects.requireNonNull(buffer, "The buffer must not be null");
        if (!buffer.isDirect()) {
            throw new IllegalArgumentException("Only direct buffers are supported");
        }
        if (length < 0) {
            throw new IllegalArgumentException("Invalid length, must be >= 0");
        }
        if (fd < 0) {
            throw new IllegalArgumentException("Invalid file descriptor, must be >= 0");
        }
        if (offset < 0) {
            throw new IllegalArgumentException("Invalid offset, must be >= 0");
        }

        this.fd = fd;
        this.length = length;
        this.buffer = buffer;
        this.offset = offset;
    }

    /**
     * The file descriptor of the file to read from.
     *
     * @return the file descriptor of this submission.
     */
    public int fd() {
        return fd;
    }

    /**
     * The number of bytes to read.
     *
     * @return the length this submission reads.
     */
    public int length() {
        return length;
    }

    /**
     * The buffer to read data into.
     *
     * @return the buffer to which the read data will be transferred.
     */
    public ByteBuffer buffer() {
        return buffer;
    }

    /**
     * The offset in the file at which to read.
     *
     * @return the file offset for this submission read.
     */
    public long offset() {
        return offset;
    }

    /**
     * Called when the submission has been completed.
     *
     * @param res the result of the operation submitted, so either the number of bytes read/written on success, or
     *            the negated {@code errno}.
     */
    public abstract void onCompletion(int res);

    @Override
    public String toString() {
        long address = NativeProvider.instance().address(buffer);
        return String.format("{fd=%d, length=%d, address=0x%x, offset=%d}", fd, length,  address, offset);
    }
}
