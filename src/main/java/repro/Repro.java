package repro;

import java.util.List;

/** Prints one variant's waves, then a RESULT line run-all.sh collects. */
public final class Repro {

    private static final double MIB = 1024.0 * 1024.0;

    private Repro() {
    }

    public static void main(final String[] args) throws Exception {
        final ReproSettings settings = ReproSettings.fromSystemProperties();
        final List<WaveResult> waves = RawRecordCacheRepro.run(settings);
        System.out.println("Variant: " + settings.label() + ", source value " + settings.valueSize()
            + " bytes, stored value " + RawRecordCacheRepro.SMALL_VALUE_LENGTH + " bytes");
        System.out.printf("%-6s %-12s %-14s %-14s %-12s%n", "keys", "live raw", "raw MB live", "cache counts MB", "heap MB");
        for (final WaveResult wave : waves) {
            System.out.printf("%-6d %-12d %-14.1f %-14.2f %-12.1f%n",
                wave.distinctKeys(), wave.liveRawArrays(), wave.liveRawBytes() / MIB,
                wave.countedCacheBytes() / MIB, wave.heapUsedAfterGcBytes() / MIB);
        }
        final WaveResult last = waves.get(waves.size() - 1);
        System.out.println("Counted size source: " + last.countedSource());
        System.out.println(resultLine(settings, last));
    }

    /** The line run-all.sh collects: label, keys, live raw arrays, raw MB live, counted MB, heap MB. */
    static String resultLine(final ReproSettings settings, final WaveResult last) {
        return String.format("RESULT|%s|%d|%d|%.1f|%.2f|%.1f", settings.label(), last.distinctKeys(),
            last.liveRawArrays(), last.liveRawBytes() / MIB, last.countedCacheBytes() / MIB,
            last.heapUsedAfterGcBytes() / MIB);
    }
}
