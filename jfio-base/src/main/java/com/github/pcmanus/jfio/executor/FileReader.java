package com.github.pcmanus.jfio.executor;

import com.github.pcmanus.jfio.NativeProvider;
import com.github.pcmanus.jfio.Submission;
import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
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
        long origOffset = offset;
        int origLength = length;
        ByteBuffer buffer;
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
            buffer = NativeProvider.instance().allocateAligned(length);
        } else {
            buffer = ByteBuffer.allocateDirect(length);
        }
        return readAsync(offset, length, buffer, origOffset, origLength);
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
        return readAsync(offset, buffer.remaining(), buffer, offset, buffer.remaining());
    }

    private CompletableFuture<ByteBuffer> readAsync(
            long offset,
            int length,
            ByteBuffer buffer,
            long origOffset,
            int origLength
    ) {
        AsyncReadSubmission submission = new AsyncReadSubmission(
                fd,
                length,
                buffer,
                offset,
                origOffset,
                origLength,
                isDirect
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
        private final boolean isDirect;

        private AsyncReadSubmission(
                int fd,
                int length,
                ByteBuffer buffer,
                long offset,
                long origOffset,
                int origLength,
                boolean isDirect
        ) {
            super(true, fd, length, buffer, offset);
            this.origOffset = origOffset;
            this.origLength = origLength;
            this.isDirect = isDirect;
        }

        private void checkDirectIOAlignments() {
            checkDirectIOAlignment(offset, "offset");
            checkDirectIOAlignment(NativeProvider.instance().address(buffer), "the buffer starting address");
            checkDirectIOAlignment(buffer.remaining(), "the buffer length");
        }

        private static void checkDirectIOAlignment(long value, String name) {
            if (value % 512 != 0) {
                throw new IllegalArgumentException(String.format("%s must be aligned on 512 bytes for direct I/O", name));
            }
        }

        @Override
        public void onCompletion(int res) {
            if (res < 0) {
                int errno = -res;
                if (errno == 22 && isDirect) {
                    // 22 is EINVAL, and is typically returned when the buffer and/or offset are not correctly aligned.
                    // So check that and give a more meaningful error message.
                    // Note that we could do those check pre-submission, but no point in taking time doing it since
                    // it's going to be checked by io_uring internally anyway.
                    try {
                        checkDirectIOAlignments();
                    } catch (IllegalArgumentException e) {
                        future.completeExceptionally(e);
                        return;
                    }
                }
                future.completeExceptionally(new IOException("Read returned error %d" + errno));
            } else {
                int pos = (int) (origOffset - offset);
                buffer.position(pos);
                buffer.limit(pos + Math.min(origLength, res));
                future.complete(buffer);
            }
        }
    }
}
