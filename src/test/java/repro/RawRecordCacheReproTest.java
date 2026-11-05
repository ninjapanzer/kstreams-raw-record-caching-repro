package repro;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * One run per JVM, configured by the Gradle properties; each test applies to one variant and is skipped
 * for the others. Run run-all.sh, or ./gradlew test with the variant's properties.
 */
class RawRecordCacheReproTest {

    private static ReproSettings settings;
    private static WaveResult last;

    @BeforeAll
    static void runTheWaves() throws Exception {
        settings = ReproSettings.fromSystemProperties();
        final List<WaveResult> waves = RawRecordCacheRepro.run(settings);
        last = waves.get(waves.size() - 1);
        System.out.println(settings.label() + ": " + last);
        System.out.println(Repro.resultLine(settings, last));
    }

    @Test
    void everyVariantSeesEveryDistinctKey() {
        assertEquals(Arrays.stream(RawRecordCacheRepro.WAVES).sum(), last.distinctKeys());
    }

    @Test
    void theCacheKeepsEverySourceValueItCannotCount() {
        assumeTrue(settings.caching() && !settings.patched() && settings.hasSourceRawRecord());
        assertTrue(last.liveRawArrays() >= 0.95 * last.distinctKeys(),
            last.liveRawArrays() + " of " + last.distinctKeys() + " source values are still reachable");
        assertTrue(last.countedCacheBytes() < 0.02 * last.liveRawBytes(),
            "cache counts " + last.countedCacheBytes() + " bytes while " + last.liveRawBytes() + " raw bytes are reachable");
    }

    @Test
    void theSourceValuesStillReachableAreHeldByCacheEntries() {
        assumeTrue(settings.caching() && !settings.patched() && settings.hasSourceRawRecord());
        assertTrue(last.heldByCacheEntries() >= 0.95 * last.liveRawArrays(),
            last.heldByCacheEntries() + " of " + last.liveRawArrays() + " live source values are referenced at "
                + CacheHolderWalk.PATH);
    }

    @Test
    void withoutRawFieldsThereIsNoHolderToWalk() {
        assumeTrue(!settings.hasSourceRawRecord());
        assertFalse(last.holderWalkApplies(), "4.0 has no source raw value on the record context");
    }

    @Test
    void withoutTheCacheNoEntryHoldsASourceValue() {
        assumeTrue(!settings.caching());
        assertEquals(0, last.heldByCacheEntries());
    }

    @Test
    void thePatchedCacheHoldsNoMoreThanItsBudget() {
        assumeTrue(settings.patched());
        assertTrue(last.heldByCacheEntriesBytes() <= 1.25 * settings.cacheMaxBytes(),
            last.heldByCacheEntriesBytes() + " bytes held by cache entries against a " + settings.cacheMaxBytes() + "-byte budget");
    }

    @Test
    void withoutRawFieldsTheCacheKeepsNoSourceValues() {
        assumeTrue(settings.caching() && !settings.hasSourceRawRecord());
        assertTrue(last.liveRawArrays() <= 0.01 * last.distinctKeys(),
            last.liveRawArrays() + " source values are still reachable");
    }

    @Test
    void withoutTheCacheNoSourceValuesAreKept() {
        assumeTrue(!settings.caching());
        assertTrue(last.liveRawArrays() <= 0.01 * last.distinctKeys(),
            last.liveRawArrays() + " source values are still reachable");
    }

    @Test
    void thePatchedCacheEvictsAtItsBudgetAndCountsWhatItKeeps() {
        assumeTrue(settings.patched());
        assertTrue(last.liveRawBytes() <= 1.25 * settings.cacheMaxBytes(),
            last.liveRawBytes() + " raw bytes reachable against a " + settings.cacheMaxBytes() + "-byte budget");
        assertTrue(Math.abs(last.countedCacheBytes() - last.liveRawBytes()) <= 0.10 * last.liveRawBytes(),
            "cache counts " + last.countedCacheBytes() + " bytes while " + last.liveRawBytes() + " raw bytes are reachable");
    }
}
