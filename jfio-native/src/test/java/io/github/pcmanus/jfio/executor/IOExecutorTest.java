package io.github.pcmanus.jfio.executor;

import io.github.pcmanus.jfio.IORing;
import io.github.pcmanus.jfio.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IOExecutorTest {
    private void canReadFile(int threadCount, IORing.Config config) throws Exception {
        try (var executor = IOExecutor.multiThreaded(threadCount, config);
             var file = executor.openForReading(TestUtils.TEST_FILE)) {

            var first = file.readAsync(0, 15);
            var second = file.readAsync(49, 18);

            Assertions.assertEquals("Maître Corbeau", TestUtils.bufferToString(first.get()));
            Assertions.assertEquals("son bec un fromage", TestUtils.bufferToString(second.get()));
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