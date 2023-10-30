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
    final Parameters parameters;

    final Submissions submissions;
    final SubmissionAndCompletionResult result;

    private boolean closed;

    IORing(Parameters parameters) {
        this.parameters = parameters;
        int depth = parameters.depth;
        this.submissions = new Submissions(depth);
        this.result = new SubmissionAndCompletionResult(submissions.maxInFlight());
    }

    public static Builder builder(int depth) {
        return new Builder(depth);
    }

    public int depth() {
        return parameters.depth;
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
     * Adds a new submission as candidate for submission by the next call to {@link #submitAndCheckCompletions}.
     * <p>
     * The submission is only added if there is room for it (see {@link #submissionSlotsAvailable}).
     *
     * @param submission the submission to add.
     * @return whether the submission was added.
     */
    public boolean add(Submission submission) {
        return submissions.add(submission);
    }

    /**
     * Attempts to submit (up to {@link #depth}) pending submissions, and then checks for any completions (of either
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
            throw new IOException(String.format("Error %d while opening fil '%s'", -fd, path));
        }
        return fd;
    }

    public abstract void closeFile(int fd);

    protected abstract void submitAndCheckCompletionsInternal();
    protected abstract void destroy();

    protected abstract int openFileInternal(MemorySegment filePathAsSegment, boolean readOnly);


    @Override
    public void close() {
        if (this.closed) {
            return;
        }

        this.closed = true;
        this.destroy();
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

        private void validateParameters() {
            if (useIOPolling && !directIO) {
                throw new IllegalArgumentException("I/O polling can only be used with direct I/O");
            }
        }

        public IORing build() {
            validateParameters();
            return new NativeIORing(new Parameters(depth, directIO, useSQPolling, useIOPolling));
        }
    }

    protected record Parameters(
            int depth,
            boolean directIO,
            boolean useSQPolling,
            boolean useIOPolling
    ) {}
}
