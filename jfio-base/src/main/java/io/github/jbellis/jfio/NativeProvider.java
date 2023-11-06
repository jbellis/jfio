package io.github.jbellis.jfio;

import java.nio.ByteBuffer;

/**
 * Provide accesses to some native operations.
 */
public abstract class NativeProvider {
    /**
     * Creates a new native provider.
     */
    protected NativeProvider() {}

    /**
     * The loaded instance of the native provider.
     * <p>
     * If the native library cannot be loaded (either because we're running on java 19 or earlier, or due to some other
     * loading error), then the returned provided methods will all throw an {@link UnavailableNativeLibraryException}
     * exception if called.
     *
     * @return the loaded provider.
     */
    public static NativeProvider instance() {
        return Holder.INSTANCE;
    }

    abstract IORing createRing(IORing.Config config);

    /**
     * Allocate a direct buffer of the given length, aligned on a 512 bytes boundary (suitable for direct I/O).
     *
     * @param length the length of the buffer to allocate.
     * @return the allocated buffer.
     */
    public abstract ByteBuffer allocateAligned(int length);

    /**
     * Returns the address in memory of the given direct buffer.
     *
     * @param buffer the buffer to get the address of.
     * @return the address in memory of the given direct buffer.
     */
    public abstract long address(ByteBuffer buffer);

    static NativeProvider lookup() {
        final int runtimeVersion = Runtime.version().feature();
        if (runtimeVersion >= 20) {
            try {
                return (NativeProvider) Class.forName("io.github.jbellis.jfio.PanamaNativeProvider").getConstructor().newInstance();
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
            throw cause == null ? new UnavailableNativeLibraryException(message) : new UnavailableNativeLibraryException(message, cause);
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
