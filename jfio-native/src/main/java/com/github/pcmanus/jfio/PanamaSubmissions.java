package com.github.pcmanus.jfio;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;

import static com.github.pcmanus.jfio.NativeUtils.ALLOCATOR;
import static com.github.pcmanus.jfio.NativeUtils.POINTER;
import static java.lang.foreign.ValueLayout.*;

class PanamaSubmissions extends Submissions {
    private static final long SUBMISSION_STRUCT_SIZE = Native.LAYOUT.byteSize();

    /** Stores that are pending; this is the submission to pass to the next `submit_and_check_completions` call */
    final MemorySegment segment;

    PanamaSubmissions(int depth) {
        super(depth);
        this.segment = Native.allocateArray(maxPending);
    }

    @Override
    void addSubmissionInternal(int index, long id, com.github.pcmanus.jfio.Submission submission) {
        Native.set(this.segment, index, id, submission);
    }

    @Override
    void move(int from, int to) {
        long fromOffset = from * SUBMISSION_STRUCT_SIZE;
        long toOffset = to * SUBMISSION_STRUCT_SIZE;
        MemorySegment.copy(this.segment, fromOffset, this.segment, toOffset, SUBMISSION_STRUCT_SIZE);
    }

    @Override
    long idOfSubmission(int index) {
        return Native.getId(this.segment, index);
    }

    static class Native {
        static final StructLayout LAYOUT;

        private static final VarHandle idVH;
        private static final VarHandle fdVH;
        private static final VarHandle bufLengthVH;
        private static final VarHandle bufBaseVH;
        private static final VarHandle offsetVH;
        private static final VarHandle isReadVH;

        static {
            LAYOUT = MemoryLayout.structLayout(
                    JAVA_LONG.withName("id"),
                    JAVA_INT.withName("fd"),
                    JAVA_INT.withName("buf_length"),
                    POINTER.withName("buf_base"),
                    JAVA_LONG.withName("offset"),
                    JAVA_BOOLEAN.withName("is_read"),
                    MemoryLayout.paddingLayout(56)
            ).withName("submission");


            idVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("id"));
            fdVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("fd"));
            bufLengthVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("buf_length"));
            bufBaseVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("buf_base"));
            offsetVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("offset"));
            isReadVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("is_read"));
        }

        static void set(MemorySegment segment, int index, long id, Submission submission) {
            MemorySegment toSet = segment.asSlice(index * LAYOUT.byteSize());

            idVH.set(toSet, id);
            fdVH.set(toSet, submission.fd());
            bufLengthVH.set(toSet, submission.length());
            bufBaseVH.set(toSet, MemorySegment.ofBuffer(submission.buffer()));
            offsetVH.set(toSet, submission.offset());
            isReadVH.set(toSet, submission.isRead());
        }

        static long getId(MemorySegment segment, int index) {
            return (long) idVH.get(segment.asSlice(index * LAYOUT.byteSize()));
        }

        static MemorySegment allocateArray(int size) {
            return ALLOCATOR.allocateArray(LAYOUT, size);
        }
    }
}
