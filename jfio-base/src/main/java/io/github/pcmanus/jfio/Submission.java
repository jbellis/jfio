package io.github.pcmanus.jfio;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * A read or write operation to be submitted to an {@link IORing}.
 */
public abstract class Submission {
    protected final boolean isRead;
    protected final int fd;
    protected final int length;
    protected final ByteBuffer buffer;
    protected final long offset;

    protected Submission(boolean isRead, int fd, int length, ByteBuffer buffer, long offset) {
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

        this.isRead = isRead;
        this.fd = fd;
        this.length = length;
        this.buffer = buffer;
        this.offset = offset;
    }

    public boolean isRead() {
        return isRead;
    }

    public int fd() {
        return fd;
    }

    public int length() {
        return length;
    }

    public ByteBuffer buffer() {
        return buffer;
    }

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
