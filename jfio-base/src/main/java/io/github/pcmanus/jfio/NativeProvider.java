package io.github.pcmanus.jfio;

import java.nio.ByteBuffer;

public abstract class NativeProvider {

    public static NativeProvider instance() {
        return Holder.INSTANCE;
    }

    public abstract IORing createRing(IORing.Config config);

    public abstract ByteBuffer allocateAligned(int length);

    public abstract long address(ByteBuffer buffer);

    static NativeProvider lookup() {
        final int runtimeVersion = Runtime.version().feature();
        if (runtimeVersion >= 20) {
            try {
                return (NativeProvider) Class.forName("io.github.pcmanus.jfio.PanamaNativeProvider").getConstructor().newInstance();
            } catch (Throwable t) {
                throw new RuntimeException("Unexpected error loading native library", t);
            }
        }
        throw new RuntimeException("Cannot load native library: make sure you are running on java 20+");
    }

    private static final class Holder {
        private Holder() {}

        static final NativeProvider INSTANCE = lookup();
    }
}
