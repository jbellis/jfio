package io.github.jbellis.jfio;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

/**
 * Native provider for jfio using Panama Foreign Function and Memory API.
 */
public class PanamaNativeProvider extends NativeProvider {
    /**
     * Creates a new FFM-based native provider.
     */
    public PanamaNativeProvider() {}

    @Override
    public IORing createRing(IORing.Config config) {
        return new PanamaIORing(config);
    }

    @Override
    public ByteBuffer allocateAligned(int length) {
        return NativeUtils.ALLOCATOR.allocate(length, 512).asByteBuffer();
    }

    @Override
    public long address(ByteBuffer buffer) {
        return MemorySegment.ofBuffer(buffer).address();
    }
}
