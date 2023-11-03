package com.github.pcmanus.jfio.executor;

import com.github.pcmanus.jfio.IORing;
import org.junit.jupiter.api.Test;

import static com.github.pcmanus.jfio.TestUtils.TEST_FILE;

import static com.github.pcmanus.jfio.TestUtils.bufferToString;
import static org.junit.jupiter.api.Assertions.*;

class IOExecutorTest {
    private void canReadFile(int threadCount, IORing.Config config) throws Exception {
        try (var executor = IOExecutor.multiThreaded(threadCount, config);
             var file = executor.openForReading(TEST_FILE)) {

            var first = file.readAsync(0, 15);
            var second = file.readAsync(49, 18);

            assertEquals("Maître Corbeau", bufferToString(first.get()));
            assertEquals("son bec un fromage", bufferToString(second.get()));
        }
    }

    @Test
    public void canReadFileWithBufferedIO() throws Exception {
        canReadFile(1, IORing.Config.buffered(2));
    }

    @Test
    public void canReadFileWithDirectIO() throws Exception {
        canReadFile(1, IORing.Config.direct(2));
    }

    @Test
    public void canReadFileWithMultipleLoop() throws Exception {
        canReadFile(2, IORing.Config.direct(2));
    }
}