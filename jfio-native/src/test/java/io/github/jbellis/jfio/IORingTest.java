package io.github.jbellis.jfio;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class IORingTest {
    @Test
    void canReadFileWithBufferedIO() throws InterruptedException, IOException {
        try (var ring = IORing.create(IORing.Config.buffered(2))) {
            int fd = ring.openFile(TestUtils.TEST_FILE, true);
            ByteBuffer buffer = ByteBuffer.allocateDirect(7);
            AtomicBoolean done = new AtomicBoolean();
            ring.add(new Submission(true, fd, 7, buffer, 4) {
                @Override
                public void onCompletion(int res) {
                    assertEquals(7, res);
                    done.set(true);
                }
            });

            // Surely, it shouldn't take more than 100ms to read 7 bytes from a file.
            for (int i = 0; i < 10; i++) {
                ring.submitAndCheckCompletions();
                if (done.get()) {
                    break;
                }
                Thread.sleep(10);
            }
            assertTrue(done.get());
            Assertions.assertEquals("tre Cor", TestUtils.bufferToString(buffer));
            ring.closeFile(fd);
        }
    }

    @Test
    void canReadFileWithDirectIO() throws IOException, InterruptedException {
        try (var ring = IORing.create(IORing.Config.direct(2))) {
            int fd = ring.openFile(TestUtils.TEST_FILE, true);
            // With direct IO, we ask for multiples of 512 bytes. So while at it, we get the whole thing.
            ByteBuffer buffer = NativeProvider.instance().allocateAligned(1024);
            AtomicBoolean done = new AtomicBoolean();
            ring.add(new Submission(true,  fd, 1024, buffer, 0) {
                @Override
                public void onCompletion(int res) {
                    assertEquals(699, res);
                    done.set(true);
                }
            });

            // Surely, it shouldn't take more than 100ms to read 7 bytes from a file.
            for (int i = 0; i < 10; i++) {
                ring.submitAndCheckCompletions();
                if (done.get()) {
                    break;
                }
                Thread.sleep(10);
            }
            assertTrue(done.get());
            buffer.limit(699);
            Assertions.assertEquals(Files.readString(TestUtils.TEST_FILE), TestUtils.bufferToString(buffer));
            ring.closeFile(fd);
        }
    }
}