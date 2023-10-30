package com.github.pcmanus.jfio.ring;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class IORingTest {
    private static final Path RESOURCES_DIR = Path.of("src", "test", "resources");
    private static final Path TEST_FILE = RESOURCES_DIR.resolve("le_corbeau_et_le_renard.txt");

    @Test
    void canReadFileWithBufferedIO() throws InterruptedException, IOException {
        try (var ring = IORing.builder(2).useDirectIO(false).build()) {
            int fd = ring.openFile(TEST_FILE, true);
            MemorySegment buffer = NativeUtils.ALLOCATOR.allocate(7);
            AtomicBoolean done = new AtomicBoolean();
            ring.submissions.add(new Submission(true, fd, 7, buffer, 4) {
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
            assertEquals("tre Cor", Charset.defaultCharset().decode(buffer.asByteBuffer()).toString());
            ring.closeFile(fd);
        }
    }

    @Test
    void canReadFileWithDirectIO() throws IOException, InterruptedException {
        try (var ring = IORing.builder(2).withDirectIO().build()) {
            int fd = ring.openFile(TEST_FILE, true);
            // With direct IO, we ask for multiples of 512 bytes. So while at it, we get the whole thing.
            MemorySegment seg = NativeUtils.ALLOCATOR.allocate(1024);
            AtomicBoolean done = new AtomicBoolean();
            ring.submissions.add(new Submission(true,  fd, 1024, seg, 0) {
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
            ByteBuffer buffer = seg.asByteBuffer();
            buffer.limit(699);
            String content = Charset.defaultCharset().decode(buffer).toString();
            assertEquals(Files.readString(TEST_FILE), content);
            ring.closeFile(fd);
        }
    }
}