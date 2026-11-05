package repro;

/** What is still reachable after one wave of distinct keys, measured after a GC. */
public record WaveResult(
    int distinctKeys,
    int liveRawArrays,
    long liveRawBytes,
    long countedCacheBytes,
    String countedSource,
    long heapUsedAfterGcBytes) {
}
