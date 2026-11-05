package repro;

import org.apache.kafka.common.serialization.Serializer;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Passes each value through unchanged, as ByteArraySerializer does, and keeps a weak reference to the
 * exact array it returns. That array is the record's raw value inside the task. The weak reference does
 * not keep it alive: it only shows whether the array survived a GC, which means something in Kafka
 * Streams still holds it through an ordinary strong reference.
 */
public final class TrackingSerializer implements Serializer<byte[]> {

    private final List<WeakReference<byte[]>> serialized = new ArrayList<>();

    @Override
    public byte[] serialize(final String topic, final byte[] data) {
        serialized.add(new WeakReference<>(data));
        return data;
    }

    public int liveArrays() {
        int live = 0;
        for (final WeakReference<byte[]> reference : serialized) {
            if (reference.get() != null) {
                live++;
            }
        }
        return live;
    }

    public long liveBytes() {
        long bytes = 0;
        for (final WeakReference<byte[]> reference : serialized) {
            final byte[] array = reference.get();
            if (array != null) {
                bytes += array.length;
            }
        }
        return bytes;
    }

    /** Live tracked arrays that are also in {@code held}, matched by identity. */
    public int liveArraysIn(final Set<byte[]> held) {
        int count = 0;
        for (final WeakReference<byte[]> reference : serialized) {
            final byte[] array = reference.get();
            if (array != null && held.contains(array)) {
                count++;
            }
        }
        return count;
    }

    /** Bytes of the live tracked arrays that are also in {@code held}, matched by identity. */
    public long liveBytesIn(final Set<byte[]> held) {
        long bytes = 0;
        for (final WeakReference<byte[]> reference : serialized) {
            final byte[] array = reference.get();
            if (array != null && held.contains(array)) {
                bytes += array.length;
            }
        }
        return bytes;
    }
}
