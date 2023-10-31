package com.github.pcmanus.jfio.ring;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;
import java.util.Objects;

import static com.github.pcmanus.jfio.ring.NativeUtils.ALLOCATOR;
import static com.github.pcmanus.jfio.ring.NativeUtils.POINTER;
import static java.lang.foreign.ValueLayout.*;

/**
 * A read or write operation to be submitted to an {@link IORing}.
 */
public abstract class Submission {
    protected final boolean isRead;
    protected final int fd;
    protected final int length;
    protected final MemorySegment buffer;
    protected final long offset;

    protected Submission(boolean isRead, int fd, int length, MemorySegment buffer, long offset) {
        Objects.requireNonNull(buffer, "The buffer must not be null");
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

    /**
     * Called when the submission has been completed.
     *
     * @param res the result of the operation submitted, so either the number of bytes read/written on success, or
     *            the negated {@code errno}.
     */
    public abstract void onCompletion(int res);

    @Override
    public String toString() {
        return String.format("{fd=%d, length=%d, address=0x%x, offset=%d}", fd, length, buffer.address(), offset);
    }

    static class Native {
        static final StructLayout LAYOUT;

        private static final VarHandle idVH;
        private static final VarHandle fdVH;
        private static final VarHandle bufLengthVH;
        private static final VarHandle bufBaseVH;
        private static final VarHandle offsetVH;
        private static final VarHandle isReadVH;

        static {
            LAYOUT = MemoryLayout.structLayout(
                    JAVA_LONG.withName("id"),
                    JAVA_INT.withName("fd"),
                    JAVA_INT.withName("buf_length"),
                    POINTER.withName("buf_base"),
                    JAVA_LONG.withName("offset"),
                    JAVA_BOOLEAN.withName("is_read"),
                    MemoryLayout.paddingLayout(56)
            ).withName("submission");


            idVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("id"));
            fdVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("fd"));
            bufLengthVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("buf_length"));
            bufBaseVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("buf_base"));
            offsetVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("offset"));
            isReadVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("is_read"));
        }

        static void set(MemorySegment segment, int index, long id, Submission submission) {
            MemorySegment toSet = segment.asSlice(index * LAYOUT.byteSize());
            idVH.set(toSet, id);
            fdVH.set(toSet, submission.fd);
            bufLengthVH.set(toSet, submission.length);
            bufBaseVH.set(toSet, submission.buffer);
            offsetVH.set(toSet, submission.offset);
            isReadVH.set(toSet, submission.isRead);
        }

        static long getId(MemorySegment segment, int index) {
            return (long) idVH.get(segment.asSlice(index * LAYOUT.byteSize()));
        }

        static MemorySegment allocateArray(int size) {
            return ALLOCATOR.allocateArray(LAYOUT, size);
        }
    }
}
