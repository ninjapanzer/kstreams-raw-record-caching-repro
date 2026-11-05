package repro;

/**
 * What is still reachable after one wave of distinct keys, measured after a GC.
 * {@code heldByCacheEntries} and {@code heldByCacheEntriesBytes} are -1 when this Kafka Streams has no
 * source raw value on the record context, so there is nothing for a cache entry to hold.
 */
public record WaveResult(
    int distinctKeys,
    int liveRawArrays,
    long liveRawBytes,
    int heldByCacheEntries,
    long heldByCacheEntriesBytes,
    long countedCacheBytes,
    String countedSource,
    long heapUsedAfterGcBytes) {

    public boolean holderWalkApplies() {
        return heldByCacheEntries >= 0;
    }
}
