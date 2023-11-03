package io.github.pcmanus.jfio;

public class UnavailableNativeLibrary extends RuntimeException {
    UnavailableNativeLibrary(String message) {
        super(message);
    }

    UnavailableNativeLibrary(String message, Throwable cause) {
        super(message, cause);
    }
}
