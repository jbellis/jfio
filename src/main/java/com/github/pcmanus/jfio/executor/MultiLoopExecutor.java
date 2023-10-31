package com.github.pcmanus.jfio.executor;

import com.github.pcmanus.jfio.ring.IORing;
import com.github.pcmanus.jfio.ring.Submission;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiLoopExecutor extends IOExecutor {
    private final EventLoop[] loops;
    private final AtomicInteger idx = new AtomicInteger(0);

    MultiLoopExecutor(EventLoop[] loops) {
        this.loops = loops;
    }

    private EventLoop next() {
        return this.loops[idx.getAndIncrement() % this.loops.length];
    }

    @Override
    public IORing.Config ringConfig() {
        // Note that we always use the same config for all the loops.
        return loops[0].ringConfig();
    }

    @Override
    void submit(Submission submission) {
        next().submit(submission);
    }

    @Override
    int openFile(Path path, boolean readonly) throws IOException {
        return next().openFile(path, readonly);
    }

    @Override
    void closeFile(int fd) throws IOException {
        next().closeFile(fd);
    }

    @Override
    public void close() {
        for (EventLoop loop : this.loops) {
            loop.close();
        }
    }
}
