package io.github.jbellis.jfio.executor;

import io.github.jbellis.jfio.IORing;
import io.github.jbellis.jfio.Submission;
import net.jcip.annotations.ThreadSafe;

import java.io.IOException;
import java.nio.file.Path;

/**
 * An executor that can be used to submit read requests.
 * <p>
 * An {@code IOExecutor} abstract one or multiple event loop threads that will be used to submit the read requests
 * and watch for their completion.
 */
@ThreadSafe
public abstract class IOExecutor implements AutoCloseable {
    IOExecutor() {}

    /**
     * Creates a new single threaded {@code IOExecutor}.
     *
     * @param ringConfig configuration for the underlying {@link IORing}.
     * @return the created executor.
     */
    public static IOExecutor singleThreaded(IORing.Config ringConfig) {
        return new EventLoop(IORing.create(ringConfig));
    }

    /**
     * Creates a new multithreaded {@code IOExecutor}.
     *
     * @param threadCount the number of threads (dedicated event loops) to use.
     * @param ringConfig configuration for the underlying {@link IORing}.
     * @return the created executor.
     */
    public static IOExecutor multiThreaded(int threadCount, IORing.Config ringConfig) {
        if (threadCount == 1) {
            return singleThreaded(ringConfig);
        }

        EventLoop[] loops = new EventLoop[threadCount];
        for (int i = 0; i < threadCount; i++) {
            loops[i] = new EventLoop(IORing.create(ringConfig));
        }
        return new MultiLoopExecutor(loops);
    }

    /**
     * Configuration for the underlying ring(s) used by this executor.
     *
     * @return the ring configuration.
     */
    public abstract IORing.Config ringConfig();

    /**
     * Creates a new {@link FileReader} for the provided path.
     *
     * @param path the path to the file to read.
     * @return the created reader.
     * @throws IOException if the file cannot be opened for reading.
     */
    public FileReader openForReading(Path path) throws IOException {
        return new FileReader(path, this);
    }

    abstract void submit(Submission submission);
    abstract int openFile(Path path) throws IOException;
    abstract void closeFile(int fd) throws IOException;

    @Override
    public abstract void close();
}
