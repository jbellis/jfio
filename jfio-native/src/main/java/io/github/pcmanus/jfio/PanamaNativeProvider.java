package io.github.pcmanus.jfio;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

public class PanamaNativeProvider extends NativeProvider {

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
