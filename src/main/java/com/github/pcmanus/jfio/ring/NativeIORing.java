package com.github.pcmanus.jfio.ring;

import net.jcip.annotations.NotThreadSafe;

import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;

import static com.github.pcmanus.jfio.ring.NativeUtils.*;
import static java.lang.foreign.ValueLayout.*;

/**
 * Main implementation of {@link IORing}, which uses Java FFM API.
 */
@NotThreadSafe
class NativeIORing extends IORing {
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
                JAVA_BOOLEAN,
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

    NativeIORing(Config config) {
        super(config);

        try {
            this.ring = (MemorySegment) createRingMH.invoke(config.depth(), config.useSQPolling(), config.useIOPolling());
            this.fileOperationsRing = (MemorySegment) createRingMH.invoke(1, false, false);
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }
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
    protected void destroy() {
        try {
            destroyRingMH.invoke(this.ring);
            destroyRingMH.invoke(this.fileOperationsRing);
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }
    }

    @Override
    protected int openFileInternal(MemorySegment filePathAsSegment, boolean readOnly) {
        try {
            return (int) openFileMH.invoke(fileOperationsRing, filePathAsSegment, readOnly, config.directIO());
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }
    }

    @Override
    public int closeFileInternal(int fd) {
        try {
            return (int) closeFileMH.invoke(fileOperationsRing, fd);
        } catch (Throwable e) {
            throw new RuntimeException("Error invoking native method", e);
        }
    }
}
