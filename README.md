# Kafka Streams record cache keeps source values it does not count

Since 4.1, Kafka Streams keeps each record's source raw key and value on its record context, and the record cache copies that context into every entry. The cache's size estimate leaves those bytes out, so a store whose values are much smaller than its source records keeps every source record it has cached, far past `statestore.cache.max.bytes`, and never evicts.

## The topology

```
events (String key, 40,000-byte value) -> mapValues to a 100-byte String -> toTable(small-store, caching on)
```

Each run pipes 7,750 distinct keys in waves of 250, 500, 1,000, 2,000 and 4,000 through `TopologyTestDriver`. The input serializer keeps a weak reference to every value it hands the driver, which is the array the task holds as the source raw value. After each wave the run forces a GC and reports how many of those arrays are still reachable, what the cache counts (`cache-size-bytes-total`), and heap used.

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

| variant                        |  keys | live raw values | raw MB reachable |  cache counts MB |      heap MB |
|--------------------------------|-------|-----------------|------------------|------------------|--------------|
| 4.2.1 cached (cache 128 MiB)   |  7750 |            7750 |            295.6 |             1.23 |        313.8 |
| 4.0.2 cached (cache 128 MiB)   |  7750 |               0 |              0.0 |             1.23 |         11.1 |
| 4.2.1 uncached (cache 128 MiB) |  7750 |               1 |              0.0 |             0.00 |          8.5 |
| 4.2.1 patched (cache 8 MiB)    |  7750 |             208 |              7.9 |             7.97 |         16.6 |

## How to read it

- On 4.2.1 with caching, every one of the 7,750 source values is still reachable: 296 MB of heap held by a cache that counts 1.23 MB, under a quarter of a percent of what it keeps. The count is the same 1.23 MB as on 4.0.2, so the cache cannot see the difference.
- 4.0.2 runs the same topology with the same cache and keeps none of them. The only change between the two rows is the Kafka Streams version, so the retained heap comes from the source raw record fields added in 4.1.
- With caching off, 4.2.1 keeps one source value: the context of the record being processed.
- The patched row counts the raw key and value in each cache entry's size. The cache then reaches its 8 MiB budget and evicts, holding 208 source values whose 7.9 MB it counts as 7.97 MB.

The fix is one method in `LRUCacheEntry`, which adds the raw key and value lengths to the entry's size: https://github.com/ninjapanzer/kafka/compare/trunk...fix-raw-record-memory-estimate. The patched variant here shadows that class with the same change.
