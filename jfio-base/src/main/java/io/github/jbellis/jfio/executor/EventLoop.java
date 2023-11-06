package io.github.jbellis.jfio.executor;

import io.github.jbellis.jfio.IORing;
import io.github.jbellis.jfio.Submission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jctools.queues.MessagePassingQueue;
import org.jctools.queues.MpscUnboundedArrayQueue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

class EventLoop extends IOExecutor {
    private static final Logger logger = LogManager.getLogger();

    private static final int QUEUE_CHUNK_SIZE = 4096;
    private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);

    private final Thread loopThread;
    private final ExecutorService fileOperationsExecutor = Executors.newSingleThreadExecutor();

    private final MessagePassingQueue<Submission> queue = new MpscUnboundedArrayQueue<>(QUEUE_CHUNK_SIZE);
    private final IORing ring;

    private volatile boolean stopped;
    private volatile boolean parked;

    EventLoop(IORing ring) {
        this.loopThread = new Thread(this::run, "EventLoop Thread #" + ID_GENERATOR.incrementAndGet());
        this.ring = ring;
        this.loopThread.start();
    }

    public IORing.Config ringConfig() {
        return ring.config();
    }

    @Override
    void submit(Submission submission) {
        if (stopped) {
            throw new IllegalStateException("This I/O executor has been closed");
        }
        boolean offered = queue.offer(submission);
        assert offered: "Queue is unbounded or what?";

        if (parked) {
            LockSupport.unpark(loopThread);
        }
    }

    @Override
    int openFile(Path path) throws IOException {
        try {
            return fileOperationsExecutor.submit(() -> ring.openFile(path)).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new RuntimeException(e);
        }
    }

    @Override
    void closeFile(int fd) throws IOException {
        try {
            fileOperationsExecutor.submit(() -> { ring.closeFile(fd); return 0; }).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
                throw (IOException) e.getCause();
            }
            throw new RuntimeException(e);
        }
    }

    private void run() {
        try {
            for (; ; ) {
                int room = ring.submissionSlotsAvailable();
                if (room > 0) {
                    queue.drain(ring::add, room);
                }
                if (ring.inFlight() == 0 && ring.pendingSubmissions() == 0) {
                    if (stopped) {
                        break;
                    }
                    // We have nothing that could be completed, and we have nothing in the queue either.
                    parked = true;
                    LockSupport.parkNanos(1000);
                    parked = false;
                } else {
                    ring.submitAndCheckCompletions();
                }
            }
            ring.close();
        } catch (Throwable e) {
            logger.error("Unexpected error during event loop", e);
        }
    }

    @Override
    public void close() {
        this.stopped = true;
        fileOperationsExecutor.shutdown();
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    this.loopThread.join();
                    fileOperationsExecutor.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS);
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
