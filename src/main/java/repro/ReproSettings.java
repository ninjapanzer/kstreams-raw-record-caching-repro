package repro;

/** One run of the reproduction, read from the {@code repro.*} system properties the Gradle build sets. */
public record ReproSettings(String kafkaVersion, boolean caching, boolean patched, long cacheMaxBytes, int valueSize) {

    static final String KAFKA_VERSION = "repro.kafkaVersion";
    static final String CACHING = "repro.caching";
    static final String PATCHED = "repro.patched";
    static final String CACHE_MAX_BYTES = "repro.cacheMaxBytes";
    static final String VALUE_SIZE = "repro.valueSize";

    public static ReproSettings fromSystemProperties() {
        return new ReproSettings(
            System.getProperty(KAFKA_VERSION, "4.2.1"),
            Boolean.parseBoolean(System.getProperty(CACHING, "true")),
            Boolean.parseBoolean(System.getProperty(PATCHED, "false")),
            Long.parseLong(System.getProperty(CACHE_MAX_BYTES, String.valueOf(128L * 1024 * 1024))),
            Integer.parseInt(System.getProperty(VALUE_SIZE, "40000")));
    }

    /** Kafka Streams added the source raw key and value to the record context in 4.1. */
    public boolean hasSourceRawRecord() {
        final String[] parts = kafkaVersion.split("\\.");
        final int major = Integer.parseInt(parts[0]);
        final int minor = Integer.parseInt(parts[1]);
        return major > 4 || (major == 4 && minor >= 1);
    }

    public String label() {
        final String mode = patched ? "patched" : caching ? "cached" : "uncached";
        return kafkaVersion + " " + mode + " (cache " + (cacheMaxBytes / (1024 * 1024)) + " MiB)";
    }
}
