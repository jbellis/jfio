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
                return new UnavailableNativeLibraryProvider("Unexpected error loading native jfio library", t);
            }
        }
        return new UnavailableNativeLibraryProvider(
                String.format("Native jfio library is only available on java 20+ (running %d)", runtimeVersion),
                null
        );
    }

    private static final class Holder {
        private Holder() {}

        static final NativeProvider INSTANCE = lookup();
    }

    private static class UnavailableNativeLibraryProvider extends NativeProvider {
        private final String message;
        private final Throwable cause;

        private UnavailableNativeLibraryProvider(String message, Throwable cause) {
            this.message = message;
            this.cause = cause;
        }

        private <T> T doThrow() {
            throw cause == null ? new UnavailableNativeLibrary(message) : new UnavailableNativeLibrary(message, cause);
        }

        @Override
        public IORing createRing(IORing.Config config) {
            return doThrow();
        }

        @Override
        public ByteBuffer allocateAligned(int length) {
            return doThrow();
        }

        @Override
        public long address(ByteBuffer buffer) {
            return doThrow();
        }
    }
}
