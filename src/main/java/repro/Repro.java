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
        System.out.printf("%-6s %-10s %-12s %-16s %-14s %-16s %-10s%n", "keys", "live raw", "raw MB live",
            "held by entries", "held MB", "cache counts MB", "heap MB");
        for (final WaveResult wave : waves) {
            System.out.printf("%-6d %-10d %-12.1f %-16s %-14s %-16.2f %-10.1f%n",
                wave.distinctKeys(), wave.liveRawArrays(), wave.liveRawBytes() / MIB,
                held(wave), heldMb(wave), wave.countedCacheBytes() / MIB, wave.heapUsedAfterGcBytes() / MIB);
        }
        System.out.println("held by entries: live source values found at " + CacheHolderWalk.PATH
            + " (strong references); cache counts: the " + RawRecordCacheRepro.CACHE_SIZE_METRIC + " metric");
        final WaveResult last = waves.get(waves.size() - 1);
        System.out.println("Counted size source: " + last.countedSource());
        System.out.println(resultLine(settings, last));
    }

    /** The line run-all.sh collects: label, keys, live raw arrays, raw MB live, held arrays, held MB, counted MB, heap MB. */
    static String resultLine(final ReproSettings settings, final WaveResult last) {
        return String.format("RESULT|%s|%d|%d|%.1f|%s|%s|%.2f|%.1f", settings.label(), last.distinctKeys(),
            last.liveRawArrays(), last.liveRawBytes() / MIB, held(last), heldMb(last),
            last.countedCacheBytes() / MIB, last.heapUsedAfterGcBytes() / MIB);
    }

    private static String held(final WaveResult wave) {
        return wave.holderWalkApplies() ? String.valueOf(wave.heldByCacheEntries()) : "n/a";
    }

    private static String heldMb(final WaveResult wave) {
        return wave.holderWalkApplies() ? String.format("%.1f", wave.heldByCacheEntriesBytes() / MIB) : "n/a";
    }
}
