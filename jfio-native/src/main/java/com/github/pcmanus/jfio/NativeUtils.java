package com.github.pcmanus.jfio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.lang.foreign.*;
import java.lang.invoke.MethodHandle;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

import static java.lang.foreign.ValueLayout.ADDRESS;

final class NativeUtils {
    private static final Logger LOG = LogManager.getLogger();

    static final int EIO_ERRNO = 5;

    static final ValueLayout.OfAddress POINTER = ADDRESS.withBitAlignment(64).asUnbounded();
    private static final Linker LINKER = Linker.nativeLinker();
    private static final SymbolLookup LOOK_BY_NAME;

    static SegmentAllocator ALLOCATOR = SegmentAllocator.nativeAllocator(SegmentScope.auto());

    static {
        SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
        LOOK_BY_NAME = name -> loaderLookup.find(name).or(() -> LINKER.defaultLookup().find(name));
    }

    private NativeUtils() {}

    /**
     * Loads a native library, either one found as a resources (included in a jar) or by calling
     * {@link System#loadLibrary(String)} if that does not work.
     *
     * @param baseName name of the library. Note that this method expects that if the library is included as a
     *                 resource, then it will be under name <pre>lib${baseName}.so</pre>.
     * @param jarLocation path to the library in the jar.
     * @throws RuntimeException if the library cannot be loaded.
     */
    static void loadNativeLibrary(String baseName, String jarLocation) {
        // First attempt to read the library from the Jar file
        String libSO = String.format("lib%s.so", baseName);
        URL libInJar = NativeUtils.class.getClassLoader().getResource(String.format("%s/%s", jarLocation, libSO));
        if (libInJar != null) {
            try {
                final File libpath = Files.createTempDirectory(baseName).toFile();
                libpath.deleteOnExit();

                File libfile = Paths.get(libpath.getAbsolutePath(), libSO).toFile();
                libfile.deleteOnExit(); // just in case

                final InputStream in = libInJar.openStream();
                final OutputStream out = new BufferedOutputStream(new FileOutputStream(libfile));

                in.transferTo(out);

                out.close();
                in.close();

                System.load(libfile.getAbsolutePath());
                LOG.debug("Loaded {} native library from {}", baseName, libfile.getAbsolutePath());
                return;
            } catch (IOException e) {
                LOG.warn("Error loading {} native library from jar: {}", baseName, e.getMessage());
            }
        }

        // If this cannot be found, or doesn't work, just try loading the library.
        try {
            System.loadLibrary(baseName);
            LOG.info("Loaded {} native library", baseName);
        } catch (Throwable e) {
            throw new RuntimeException("Native library " + baseName + " not found, or cannot be loaded", e);
        }
    }

    static MethodHandle lookupNativeFunction(String name, FunctionDescriptor descriptor) {
        return LOOK_BY_NAME
                .find(name)
                .map(addr -> LINKER.downcallHandle(addr, descriptor))
                .orElseThrow(() -> new RuntimeException("Error finding/loading symbol: " + name));
    }
}
