package io.github.jbellis.jfio;

import net.jcip.annotations.NotThreadSafe;

import java.io.IOException;
import java.nio.file.Path;

/**
 * An io_uring ring to which reads can be submitted.
 * <p>
 * This underlying ring (submission and completion queues) is created upon the construction of an {@code IORing} and
 * destroyed upon closing it.
 * <p>
 * Submissions (reads at the moment) can be added to {@link #submissions} and then submitted to the ring by calling
 * {@link #submitAndCheckCompletions}, which both tries to submit anything in {@link #submissions}
 * and to check for any completion.
 */
@NotThreadSafe
public abstract class IORing implements AutoCloseable {
    final Config config;

    private boolean closed;

    IORing(Config config) {
        this.config = config;
    }

    /**
     * Creates a new ring based on the provided config.
     *
     * @param config the configuration for the created ring.
     * @return the created ring.
     */
    public static IORing create(Config config) {
        return NativeProvider.instance().createRing(config);
    }

    /**
     * The configuration of this ring.
     *
     * @return the ring configuration.
     */
    public Config config() {
        return config;
    }

    /**
     * How many submissions are currently in flight, that is genuinely submitted to the kernel but not yet completed
     * (or rather, whose completion hasn't been seen by {@link #submitAndCheckCompletions}).
     *
     * @return the number of currently in-flight submissions.
     */
    public int inFlight() {
        return submissions().inFlight();
    }

    /**
     * How many submissions can be in flight at any given time.
     *
     * @return the maximum number in-flight submissions this ring supports.
     */
    public int maxInFlight() {
        return submissions().maxInFlight();
    }

    /**
     * How many additional submissions can be added to {@link #submissions}, and have a chance to be submitted by
     * the next {@link #submitAndCheckCompletions} call.
     *
     * @return how many new submissions can be added by {@link #add(Submission)}. When this reaches 0, the
     * {@link #submitAndCheckCompletions()} method needs to be call to submit pending submissions.
     */
    public int submissionSlotsAvailable() {
        return submissions().room();
    }

    /**
     * The number of submissions that have been added to the ring (through {@link #add}) but haven't yet been
     * successfully submitted to the underlying io_uring ring (which should happen by calling
     * {@link #submitAndCheckCompletions()}).
     *
     * @return the number of submissions added for submission but not yet submitted to the underlying ring.
     */
    public int pendingSubmissions() {
        return submissions().pending();
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
        return submissions().add(submission);
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
        int submitted = submitted();
        int completed = completed();
        submissions().onSubmitted(submitted);

        for (int i = 0; i < completed; i++) {
            submissions().onCompleted(completedId(i), completedRes(i));
        }
    }

    /**
     * Open the provided file (for reading only) and return the underlying "native" file descriptor.
     *
     * @param path the file to open.
     * @return the file descriptor of the file.
     *
     * @throws IOException if the file cannot be open
     */
    public abstract int openFile(Path path) throws IOException;

    /**
     * Closes the provided field descriptor (as obtained by {@link #openFile}).
     *
     * @param fd the file descriptor to close.
     * @throws IOException if the file cannot be closed.
     */
    public abstract void closeFile(int fd) throws IOException;

    abstract Submissions submissions();

    /** Actually submits the pending submissions, and reap completions. */
    protected abstract void submitAndCheckCompletionsInternal();

    /**
     * Number of pending submissions submitted by the last {@link #submitAndCheckCompletionsInternal} call.
     *
     * @return how many pending submissions were last submitted.
     */
    protected abstract int submitted();

    /**
     * Number of completions witnessed by the last {@link #submitAndCheckCompletionsInternal} call.
     *
     * @return how many completions were last witnessed.
     */
    protected abstract int completed();

    /**
     * Return completion ids witnessed by the last {@link #submitAndCheckCompletionsInternal} call.
     *
     * @param i the index of the completion to query.
     * @return the id of the {@code i}th completion witnessed.
     */
    protected abstract int completedId(int i);

    /**
     * Return completion results (completion queue {@code res} field) witnessed by the last {@link #submitAndCheckCompletionsInternal} call.
     *
     * @param i the index of the completion to query.
     * @return the {@code res} value of the {@code i}th completion witnessed.
     */
    protected abstract int completedRes(int i);

    /**
     * Destroys the underlying ring.
     */
    protected abstract void destroy();

    @Override
    public void close() {
        if (this.closed) {
            return;
        }

        this.closed = true;
        this.destroy();
    }

    /**
     * Configuration of an {@link IORing}.
     */
    public static class Config {
        private final int depth;
        private final boolean directIO;
        private final boolean useSQPolling;
        private final boolean useIOPolling;

        private Config(int depth, boolean directIO, boolean useSQPolling, boolean useIOPolling) {
            this.depth = depth;
            this.directIO = directIO;
            this.useSQPolling = useSQPolling;
            this.useIOPolling = useIOPolling;
        }

        /**
         * Creates a configuration for a ring using buffered I/O with the provided depth and otherwise default
         * configuration.
         *
         * @param depth the depth of the ring.
         * @return the created configuration.
         */
        public static Config buffered(int depth) {
            return builder(depth).build();
        }

        /**
         * Creates a configuration for a ring using direct I/O with the provided depth and otherwise default
         * configuration.
         *
         * @param depth the depth of the ring.
         * @return the created configuration.
         */
        public static Config direct(int depth) {
            return builder(depth).withDirectIO().build();
        }

        /**
         * Creates a configuration builder for a ring using the provided depth.
         *
         * @param depth the ring depth for the configuration built by the returned builder.
         * @return the created builder.
         */
        public static Builder builder(int depth) {
            return new Builder(depth);
        }

        /**
         * The depth of this ring configuration.
         *
         * @return the configured ring depth (technically, number of entries in the underlying submission queue).
         */
        public int depth() {
            return depth;
        }

        /**
         * Whether this ring configuration uses direct I/O.
         *
         * @return whether direct I/O is used.
         */
        public boolean directIO() {
            return directIO;
        }

        /**
         * Whether this ring configuration uses kernel-side submission queue polling.
         *
         * @return whether submission queue polling is used.
         */
        public boolean useSQPolling() {
            return useSQPolling;
        }

        /**
         * Whether this ring configuration uses I/O polling.
         *
         * @return whether I/O polling is used.
         */
        public boolean useIOPolling() {
            return useIOPolling;
        }

        /**
         * Builder for ring configurations.
         */
        public static class Builder {
            private final int depth;
            private boolean directIO = false;
            private boolean useIOPolling = false;
            private boolean useSQPolling = false;

            Builder(int depth) {
                if (depth <= 0) {
                    throw new IllegalArgumentException("Depth must be positive");
                }
                this.depth = depth;
            }

            /**
             * Sets up the configuration to use direct I/O.
             *
             * @return this builder.
             */
            public Builder withDirectIO() {
                this.directIO = true;
                return this;
            }

            /**
             * Sets whether the configuration will use direct I/O or not.
             *
             * @param useDirectIO whether to use direct I/O or not.
             * @return this builder.
             */
            public Builder useDirectIO(boolean useDirectIO) {
                this.directIO = useDirectIO;
                return this;
            }

            /**
             * Sets up the configuration to use I/O Polling.
             * <p>
             * This is only supported/allowed if the configuration uses direct I/O. Further, for this to actually work,
             * you'll probably need to enable `poll_queues` for the NVMe linux driver.
             *
             * @return this builder.
             */
            public Builder withIOPolling() {
                this.useIOPolling = true;
                return this;
            }

            /**
             * Sets whether the configuration will use I/O polling or not.
             * <p>
             * See {@link #withIOPolling} for restrictions/considerations regarding I/O polling.
             *
             * @param useIOPolling whether to use I/O polling or not.
             * @return this builder.
             */
            public Builder useIOPolling(boolean useIOPolling) {
                this.useIOPolling = useIOPolling;
                return this;
            }

            /**
             * Sets up the configuration to use (kernel-side) submission queue polling.
             *
             * @return this builder.
             */
            public Builder withSQPolling() {
                this.useSQPolling = true;
                return this;
            }

            /**
             * Sets whether the configuration will use submission queue polling or not.
             *
             * @param useSQPolling whether to use submission queue polling or not.
             * @return this builder.
             */
            public Builder useSQPolling(boolean useSQPolling) {
                this.useSQPolling = useSQPolling;
                return this;
            }

            private void validate() {
                if (useIOPolling && !directIO) {
                    throw new IllegalArgumentException("I/O polling can only be used with direct I/O");
                }
            }

            /**
             * Build the configuration corresponding to the state of this builder.
             *
             * @return the built configuration.
             * @throws IllegalArgumentException if the configuration is invalid.
             */
            public Config build() {
                validate();
                return new Config(depth, directIO, useSQPolling, useIOPolling);
            }
        }
    }

}
