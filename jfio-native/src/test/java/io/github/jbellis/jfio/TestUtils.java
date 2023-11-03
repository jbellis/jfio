package io.github.jbellis.jfio;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Path;

public class TestUtils {
    public static final Path RESOURCES_DIR = Path.of("src", "test", "resources");
    public static final Path TEST_FILE = RESOURCES_DIR.resolve("le_corbeau_et_le_renard.txt");

    public static String bufferToString(ByteBuffer buffer) {
        return Charset.defaultCharset().decode(buffer).toString();
    }
}
