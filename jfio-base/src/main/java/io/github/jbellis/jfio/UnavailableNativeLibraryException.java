package io.github.jbellis.jfio;

/**
 * Exception thrown when the JFIO native library is not available.
 */
public class UnavailableNativeLibraryException extends RuntimeException {
    UnavailableNativeLibraryException(String message) {
        super(message);
    }

    UnavailableNativeLibraryException(String message, Throwable cause) {
        super(message, cause);
    }
}
