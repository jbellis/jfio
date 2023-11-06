package io.github.jbellis.jfio;

import net.jcip.annotations.NotThreadSafe;

import java.io.IOException;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;

import static io.github.jbellis.jfio.NativeUtils.lookupNativeFunction;
import static java.lang.foreign.ValueLayout.*;

/**
 * Main implementation of {@link IORing}, which uses Java FFM API.
 */
@NotThreadSafe
class PanamaIORing extends IORing {
    private static final OfAddress POINTER = ADDRESS.withBitAlignment(64).asUnbounded();

    private static final MethodHandle submitAndCheckCompletionsMH;

    private static final MethodHandle createRingMH;
    private static final MethodHandle destroyRingMH;

    private static final MethodHandle openFileMH;
    private static final MethodHandle closeFileMH;

    static {
        NativeUtils.loadNativeLibrary("jfio", "native-lib");

        FunctionDescriptor submitAndCheckCompletionsDesc = FunctionDescriptor.ofVoid(
                POINTER,
                POINTER,
                JAVA_INT,
                POINTER
        );
        submitAndCheckCompletionsMH = lookupNativeFunction("submit_and_check_completions", submitAndCheckCompletionsDesc);

        FunctionDescriptor createRingDesc = FunctionDescriptor.of(
                POINTER,
                JAVA_INT,
                JAVA_BOOLEAN,
                JAVA_BOOLEAN
        );
        createRingMH = lookupNativeFunction("create_ring", createRingDesc);


        FunctionDescriptor destroyRingDesc = FunctionDescriptor.ofVoid(POINTER);
        destroyRingMH = lookupNativeFunction("destroy_ring", destroyRingDesc);

        FunctionDescriptor openFileDesc = FunctionDescriptor.of(
                JAVA_INT,
                POINTER,
                POINTER,
                JAVA_BOOLEAN
        );
        openFileMH = lookupNativeFunction("open_file", openFileDesc);

        FunctionDescriptor closeFileDesc = FunctionDescriptor.of(
                JAVA_INT,
                POINTER,
                JAVA_INT
        );
        closeFileMH = lookupNativeFunction("close_file", closeFileDesc);
    }

    private final MemorySegment ring;
    private final MemorySegment fileOperationsRing;
    private final PanamaSubmissions submissions;
    private final SubmissionAndCompletionResult result;

    PanamaIORing(Config config) {
        super(config);
        try {
            this.ring = (MemorySegment) createRingMH.invoke(config.depth(), config.useSQPolling(), config.useIOPolling());
            this.fileOperationsRing = (MemorySegment) createRingMH.invoke(1, false, false);
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }

        this.submissions = new PanamaSubmissions(config.depth());
        this.result = new SubmissionAndCompletionResult(submissions.maxInFlight());
    }

    @Override
    protected void submitAndCheckCompletionsInternal() {
        try {
            submitAndCheckCompletionsMH.invoke(
                    this.ring,
                    this.submissions.segment,
                    this.submissions.pending(),
                    this.result.segment
            );
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }
    }

    @Override
    Submissions submissions() {
        return submissions;
    }

    @Override
    protected int submitted() {
        return result.submitted();
    }

    @Override
    protected int completed() {
        return result.completed();
    }

    @Override
    protected int completedId(int i) {
        return result.id(i);
    }

    @Override
    protected int completedRes(int i) {
        return result.res(i);
    }

    @Override
    protected void destroy() {
        try {
            destroyRingMH.invoke(this.ring);
            destroyRingMH.invoke(this.fileOperationsRing);
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }
    }

    @Override
    public int openFile(Path path) throws IOException {
        String absolutePath = path.toAbsolutePath().toString();
        int length = absolutePath.length();
        MemorySegment segment = NativeUtils.ALLOCATOR.allocate(length + 1);
        segment.setUtf8String(0, absolutePath);
        int fd = openFileInternal(segment);
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

    private int openFileInternal(MemorySegment filePathAsSegment) {
        try {
            return (int) openFileMH.invoke(fileOperationsRing, filePathAsSegment, config.directIO());
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }
    }

    @Override
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

    private int closeFileInternal(int fd) {
        try {
            return (int) closeFileMH.invoke(fileOperationsRing, fd);
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }
    }
}
