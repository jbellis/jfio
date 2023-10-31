package com.github.pcmanus.jfio.ring;

import net.jcip.annotations.NotThreadSafe;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.file.Path;

/**
 * An io_uring ring to which read and writes can be submitted.
 * <p>
 * This underlying ring (submission and completion queues) is created upon the construction of an {@code IORing} and
 * destroyed upon closing it.
 * <p>
 * Submissions (read or write) can be added to {@link #submissions} and then submitted to the ring by calling
 * {@link #submitAndCheckCompletions}, which both tries to submit anything in {@link #submissions}
 * and to check for any completion.
 */
@NotThreadSafe
public abstract class IORing implements AutoCloseable {
    final Config config;

    final Submissions submissions;
    final SubmissionAndCompletionResult result;

    private boolean closed;

    IORing(Config config) {
        this.config = config;
        int depth = config.depth;
        this.submissions = new Submissions(depth);
        this.result = new SubmissionAndCompletionResult(submissions.maxInFlight());
    }

    public static IORing create(Config config) {
        return new NativeIORing(config);
    }

    public Config config() {
        return config;
    }

    /**
     * How many submissions are currently in flight, that is genuinely submitted to the kernel but not yet completed
     * (or rather, whose completion hasn't been seen by {@link #submitAndCheckCompletions}).
     */
    public int inFlight() {
        return submissions.inFlight();
    }

    /**
     * How many submissions can be in flight at any given time.
     */
    public int maxInFlight() {
        return submissions.maxInFlight();
    }

    /**
     * How many additional submissions can be added to {@link #submissions}, and have a chance to be submitted by
     * the next {@link #submitAndCheckCompletions} call.
     */
    public int submissionSlotsAvailable() {
        return submissions.room();
    }

    /**
     * The number of submissions that have been added to the ring (through {@link #add}) but haven't yet been
     * successfully submitted to the underlying io_uring ring (which should happen by calling
     * {@link #submitAndCheckCompletions()}).
     */
    public int pendingSubmissions() {
        return submissions.pending();
    }

    /**
     * Adds a new submission as candidate for submission by the next call to {@link #submitAndCheckCompletions}.
     * <p>
     * The submission is only added if there is room for it (see {@link #submissionSlotsAvailable}).
     *
     * @param submission the submission to add. Please note that if direct I/O is used, the submission must respect
     *                   direct I/O constraints (namely, the buffer address, offset and length must be aligned on 512
     *                   bytes). It not, the submission will ultimately complete with a 22 (EINVAL) error code.
     * @return whether the submission was added.
     *
     * @throws IllegalArgumentException if the submission is invalid for the ring configuration. Mostly, when using
     *   direct I/O, the constraints are that the buffer address, the offset and the length must all be aligned on 512
     *   bytes.
     */
    public boolean add(Submission submission) {
        return submissions.add(submission);
    }

    /**
     * Attempts to submit (up to {@code this.config().depth()}) pending submissions, and then checks for any completions (of either
     * previous or new submissions).
     * <p>
     * Any submission completed will have it's {@link Submission#onCompletion} method called by this method.
     */
    public void submitAndCheckCompletions() {
        if (this.closed) {
            throw new IllegalStateException("Ring is closed");
        }

        this.submitAndCheckCompletionsInternal();
        int submitted = result.submitted();
        int completed = result.completed();
        submissions.onSubmitted(submitted);

        for (int i = 0; i < completed; i++) {
            submissions.onCompleted(result.id(i), result.res(i));
        }
    }

    public int openFile(Path path, boolean readOnly) throws IOException {
        String absolutePath = path.toAbsolutePath().toString();
        int length = absolutePath.length();
        MemorySegment segment = NativeUtils.ALLOCATOR.allocate(length + 1);
        segment.setUtf8String(0, absolutePath);
        int fd = openFileInternal(segment, readOnly);
        if (fd < 0) {
            int errno = -fd;
            if (errno == NativeUtils.EIO_ERRNO) {
                throw new IOException(String.format("Error opening file '%s': I/O error", path));
            } else {
                throw new RuntimeException(String.format("Unexpected error opening file '%s' (errno: %d)", path, errno));
            }
        }
        return fd;
    }

    public void closeFile(int fd) throws IOException {
        int res = closeFileInternal(fd);
        if (res < 0) {
            int errno = -res;
            if (errno == NativeUtils.EIO_ERRNO) {
                throw new IOException("Error closing file: I/O error");
            } else {
                throw new RuntimeException("Unexpected error closing file (errno: " + errno + ")");
            }
        }
    }

    protected abstract void submitAndCheckCompletionsInternal();
    protected abstract void destroy();

    protected abstract int openFileInternal(MemorySegment filePathAsSegment, boolean readOnly);
    protected abstract int closeFileInternal(int fd);


    @Override
    public void close() {
        if (this.closed) {
            return;
        }

        this.closed = true;
        this.destroy();
    }

    public record Config(
            int depth,
            boolean directIO,
            boolean useSQPolling,
            boolean useIOPolling
    ) {
        public static Config buffered(int depth) {
            return builder(depth).build();
        }

        public static Config direct(int depth) {
            return builder(depth).withDirectIO().build();
        }

        public static Builder builder(int depth) {
            return new Builder(depth);
        }

        public static class Builder {
            private final int depth;
            private boolean directIO = false;
            private boolean useIOPolling = false;
            private boolean useSQPolling = false;

            public Builder(int depth) {
                if (depth <= 0) {
                    throw new IllegalArgumentException("Depth must be positive");
                }
                this.depth = depth;
            }

            public Builder withDirectIO() {
                this.directIO = true;
                return this;
            }

            public Builder useDirectIO(boolean useDirectIO) {
                this.directIO = useDirectIO;
                return this;
            }

            public Builder withIOPolling() {
                this.useIOPolling = true;
                return this;
            }

            public Builder useIOPolling(boolean useIOPolling) {
                this.useIOPolling = useIOPolling;
                return this;
            }

            public Builder withSQPolling() {
                this.useSQPolling = true;
                return this;
            }

            public Builder useSQPolling(boolean useSQPolling) {
                this.useSQPolling = useSQPolling;
                return this;
            }

            private void validate() {
                if (useIOPolling && !directIO) {
                    throw new IllegalArgumentException("I/O polling can only be used with direct I/O");
                }
            }

            public Config build() {
                validate();
                return new Config(depth, directIO, useSQPolling, useIOPolling);
            }
        }
    }

}
