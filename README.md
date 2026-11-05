# Kafka Streams record cache keeps source values it does not count

Since 4.1, Kafka Streams keeps each record's source raw key and value on its record context, and the record cache copies that context into every entry. The cache's size estimate leaves those bytes out, so a store whose values are much smaller than its source records keeps every source record it has cached, far past `statestore.cache.max.bytes`, and never evicts.

## The topology

```
events (String key, 40,000-byte value) -> mapValues to a 100-byte String -> toTable(small-store, caching on)
```

Each run pipes 7,750 distinct keys in waves of 250, 500, 1,000, 2,000 and 4,000 through `TopologyTestDriver`. After each wave the run forces a GC and measures three things:

- **Live raw values:** how many source values survived the GC. The input serializer keeps a `WeakReference` to every value it hands the driver. A weak reference does not keep the array alive, so a survivor is one that something in Kafka Streams holds with an ordinary strong reference.
- **Held by cache entries:** which of those survivors a cache entry holds. The run walks the cache's internals by reflection and matches each `LRUCacheEntry.record` → `ContextualRecord.recordContext` → `ProcessorRecordContext.sourceRawValue` against the survivors by identity.
- **Cache counts:** what the cache believes it holds, from the `cache-size-bytes-total` metric.

## Run it

```
./run-all.sh
```

That runs the tests for four variants and prints the table below; it fails if any variant's assertions fail. To resolve dependencies from another Maven repository, set `MAVEN_REPO=<url>`. One variant, with its per-wave table:

```
./gradlew run -PkafkaVersion=4.2.1                 # 4.2.1, caching on
./gradlew run -PkafkaVersion=4.0.2                 # before the raw record fields existed
./gradlew run -PkafkaVersion=4.2.1 -Pcaching=false # 4.2.1, caching off
./gradlew run -PkafkaVersion=4.2.1 -Ppatched=true  # 4.2.1 with the fix, 8 MiB cache
```

## Result

JDK 25, default settings:

| variant                        |  keys | live raw values | raw MB reachable | held by cache entries |         held MB | cache counts MB |  heap MB |
|--------------------------------|-------|-----------------|------------------|----------------------|-----------------|-----------------|----------|
| 4.2.1 cached (cache 128 MiB)   |  7750 |            7750 |            295.6 |                 7750 |           295.6 |            1.23 |    321.7 |
| 4.0.2 cached (cache 128 MiB)   |  7750 |               0 |              0.0 |                  n/a |             n/a |            1.23 |     11.2 |
| 4.2.1 uncached (cache 128 MiB) |  7750 |               1 |              0.0 |                    0 |             0.0 |            0.00 |      8.5 |
| 4.2.1 patched (cache 8 MiB)    |  7750 |             208 |              7.9 |                  208 |             7.9 |            7.97 |     16.7 |

## How to read it

- On 4.2.1 with caching, every one of the 7,750 source values is still reachable, and every one is held by a cache entry's record context: 296 MB that the cache counts as 1.23 MB, under a quarter of a percent. The count is the same 1.23 MB as on 4.0.2, so the cache cannot see the difference.
- 4.0.2 runs the same topology with the same cache and keeps none of them. Its record context has no source raw value, so there is nothing for an entry to hold (`n/a`). The only change between the two rows is the Kafka Streams version, so the retained heap comes from the source raw record fields added in 4.1.
- With caching off, 4.2.1 keeps one source value, the context of the record being processed, and no cache entry holds any.
- The patched row counts the raw key and value in each cache entry's size. The cache then reaches its 8 MiB budget and evicts, holding 208 source values whose 7.9 MB it counts as 7.97 MB.

## See the reference chain in a heap dump

```
./gradlew test -PkafkaVersion=4.2.1 -PheapDump=true
```

writes `build/heapdumps/4.2.1-cached.hprof` (live objects only, about 340 MB) after the last wave; `HEAP_DUMP=true ./run-all.sh` writes one per variant. Open it in Eclipse MAT, find a `byte[]` of 40,000 bytes (OQL: `SELECT * FROM byte[] b WHERE b.@length = 40000`), and run **Path To GC Roots → exclude weak/soft references** on it. On 4.2.1 the path reads:

```
byte[40000]
|- sourceRawValue  ProcessorRecordContext
|  |- recordContext  ContextualRecord
|  |  |- record  LRUCacheEntry
|  |  |  |- entry  NamedCache$LRUNode
|  |  |  |  |- ... TreeMap$Entry ... root  TreeMap
|  |  |  |  |  |- cache  NamedCache
|  |  |  |  |  |  |- ... caches  ThreadCache
|  |  |  |  |  |  |  |- cache  ProcessorContextImpl
|  |  |  |  |  |  |  |  |- processorContext  StreamTask
|  |  |  |  |  |  |  |  |  |- task  TopologyTestDriver
```

The class histogram of the same dump has 7,750 `LRUCacheEntry`, `ContextualRecord` and `NamedCache$LRUNode` instances, one per distinct key.

## The same chain from a running application, without a heap dump

Java Flight Recorder samples long-lived objects and records the reference chain to a GC root:

```
jcmd <pid> JFR.start name=cache settings=profile path-to-gc-roots=true
# let it run while the heap grows, then:
jcmd <pid> JFR.dump name=cache filename=cache.jfr
jfr print --events jdk.OldObjectSample cache.jfr
```

## The fix

The fix is one method in `LRUCacheEntry`, which adds the raw key and value lengths to the entry's size: https://github.com/ninjapanzer/kafka/compare/trunk...fix-raw-record-memory-estimate. The patched variant here shadows that class with the same change.
