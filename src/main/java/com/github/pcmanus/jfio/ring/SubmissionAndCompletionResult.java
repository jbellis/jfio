package com.github.pcmanus.jfio.ring;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.VarHandle;

import static com.github.pcmanus.jfio.ring.NativeUtils.ALLOCATOR;
import static com.github.pcmanus.jfio.ring.NativeUtils.POINTER;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

class SubmissionAndCompletionResult {
    final MemorySegment segment;

    SubmissionAndCompletionResult(int maxCompleted) {
        this.segment = Native.allocate();

        Native.setCompletedRes(this.segment, ALLOCATOR.allocateArray(JAVA_INT, maxCompleted));
        Native.setCompletedIds(this.segment, ALLOCATOR.allocateArray(JAVA_LONG, maxCompleted));
    }

    int submitted() {
        return Native.submitted(this.segment);
    }

    int completed() {
        return Native.completed(this.segment);
    }

    int res(int i) {
        return Native.completedRes(this.segment, i);
    }

    int id(int i) {
        return (int) Native.completedIds(this.segment, i);
    }

    static class Native {
        static final StructLayout LAYOUT;

        private static final VarHandle nrSubmittedVH;
        private static final VarHandle nrCompletedVH;
        private static final VarHandle completedResVH;
        private static final VarHandle completedIdsVH;

        static {
            LAYOUT = MemoryLayout.structLayout(
                    JAVA_INT.withName("nr_submitted"),
                    JAVA_INT.withName("nr_completed"),
                    POINTER.withName("completed_res"),
                    POINTER.withName("completed_ids")
            ).withName("submission_and_completion_result");

            nrSubmittedVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("nr_submitted"));
            nrCompletedVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("nr_completed"));
            completedResVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("completed_res"));
            completedIdsVH = LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("completed_ids"));
        }

        static int submitted(MemorySegment seg) {
            return (int) nrSubmittedVH.get(seg);
        }

        static int completed(MemorySegment seg) {
            return (int) nrCompletedVH.get(seg);
        }

        static void setCompletedRes(MemorySegment seg, MemorySegment resBuffer) {
            completedResVH.set(seg, resBuffer);
        }

        static int completedRes(MemorySegment seg, int i) {
            return ((MemorySegment) completedResVH.get(seg)).getAtIndex(JAVA_INT, i);
        }

        static void setCompletedIds(MemorySegment seg, MemorySegment idsBuffer) {
            completedIdsVH.set(seg, idsBuffer);
        }

        static long completedIds(MemorySegment seg, int i) {
            return ((MemorySegment) completedIdsVH.get(seg)).getAtIndex(JAVA_LONG, i);
        }

        static MemorySegment allocate() {
            return ALLOCATOR.allocate(LAYOUT);
        }
    }
}
