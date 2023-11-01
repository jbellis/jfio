package com.github.pcmanus.jfio.executor;

import com.github.pcmanus.jfio.ring.Submission;
import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SegmentScope;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

@ThreadSafe
public class FileReader implements AutoCloseable {
    private final Path path;
    private final IOExecutor executor;
    private final boolean isDirect;

    private final int fd;

    FileReader(Path path, IOExecutor executor) throws IOException {
        this.path = path;
        this.executor = executor;
        this.isDirect = executor.ringConfig().directIO();
        this.fd = executor.openFile(path, true);
    }

    public Path path() {
        return path;
    }

    /**
     * Submits an asynchronous read request to the underlying {@link IOExecutor}.
     * <p>
     * Please note that this method imposes no "alignment" constraints on the offset and length parameters for
     * convenience, but if the underlying executor uses direct I/O and the offset and length are not aligned on 512
     * bytes, then the underlying read will be extended to the nearest 512 bytes boundary (this is transparent
     * in the sense that the returned {@link ByteBuffer} will have its position and limit properly set to expose
     * only what {@code offset} and {@code length} covers, but it means this method may somewhat over-read under
     * the hood in that case).
     *
     * @param offset the offset for the read.
     * @param length the length to read.
     * @return a future on the result of the read.
     */
    public CompletableFuture<ByteBuffer> readAsync(long offset, int length) {
        long byteAlignment = isDirect ? 512 : 1;
        long origOffset = offset;
        int origLength = length;
        if (isDirect) {
            int offsetMod = (int) offset % 512;
            if (offsetMod != 0) {
                offset -= offsetMod;
                length += offsetMod;
            }
            int lengthMod = length % 512;
            if (lengthMod != 0) {
                length += 512 - lengthMod;
            }
        }
        MemorySegment segment = MemorySegment.allocateNative(length, byteAlignment, SegmentScope.auto());
        return readAsync(offset, segment, segment.asByteBuffer(), origOffset, origLength);
    }

    /**
     * Submits an asynchronous read request to the underlying {@link IOExecutor}.
     * <p>
     * If the underlying executor uses direct I/O, then the arguments to this method must respect a few constraints:
     *  - the offset must be aligned on 512 bytes.
     *  - the buffer must be a direct byte buffer aligned on 512 bytes, and it's length must also be a multiple of 512.
     *
     * @param offset the offset for the read.
     * @param buffer the buffer to read into; length of the read will be that of the buffer remaining bytes.
     * @return a future on the result of the read.
     */
    public CompletableFuture<ByteBuffer> readAsync(long offset, ByteBuffer buffer) {
        MemorySegment segment = MemorySegment.ofBuffer(buffer);
        if (isDirect) {
            if (!buffer.isDirect()) {
                throw new IllegalArgumentException("buffer must be direct as the executor uses direct I/O");
            }
            ensureDirectIOAlignment(offset, "offset");
            ensureDirectIOAlignment(segment.address(), "the buffer starting address");
            ensureDirectIOAlignment(segment.byteSize(), "the buffer length");
        }
        return readAsync(offset, segment, buffer, offset, buffer.remaining());
    }

    private void ensureDirectIOAlignment(long value, String name) {
        if (value % 512 != 0) {
            throw new IllegalArgumentException(name + " must be aligned on 512 bytes");
        }
    }

    private CompletableFuture<ByteBuffer> readAsync(
            long offset,
            MemorySegment segment,
            ByteBuffer result,
            long origOffset,
            int origLength
    ) {
        AsyncReadSubmission submission = new AsyncReadSubmission(
                fd,
                result.remaining(),
                segment,
                offset,
                origOffset,
                origLength,
                result
        );
        executor.submit(submission);
        return submission.future;
    }

    @Override
    public void close() throws IOException {
        executor.closeFile(fd);
    }

    private static class AsyncReadSubmission extends Submission {
        private final CompletableFuture<ByteBuffer> future = new CompletableFuture<>();

        private final long origOffset;
        private final int origLength;

        private final ByteBuffer result;

        protected AsyncReadSubmission(
                int fd,
                int length,
                MemorySegment buffer,
                long offset,
                long origOffset,
                int origLength,
                ByteBuffer result
        ) {
            super(true, fd, length, buffer, offset);
            this.origOffset = origOffset;
            this.origLength = origLength;
            this.result = result;
        }

        @Override
        public void onCompletion(int res) {
            if (res < 0) {
                future.completeExceptionally(new IOException("Read returned error " + -res));
            } else {
                int pos = (int) (origOffset - offset);
                result.position(pos);
                result.limit(pos + Math.min(origLength, res));
                future.complete(result);
            }
        }
    }
}
