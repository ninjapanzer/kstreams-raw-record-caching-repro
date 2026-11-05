package repro;

import com.sun.management.HotSpotDiagnosticMXBean;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;

/** Writes a heap dump of live objects only, so a heap analyzer opens it with the GC already applied. */
final class HeapDump {

    private HeapDump() {
    }

    static void write(final Path file) throws IOException {
        Files.createDirectories(file.toAbsolutePath().getParent());
        Files.deleteIfExists(file);
        ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class).dumpHeap(file.toString(), true);
        System.out.println("Heap dump: " + file.toAbsolutePath());
    }
}
