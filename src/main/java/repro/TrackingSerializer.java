package repro;

import org.apache.kafka.common.serialization.Serializer;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;

/**
 * Passes each value through unchanged, as ByteArraySerializer does, and keeps a weak reference to the
 * exact array it returns. That array is the record's raw value inside the task, so a reference that
 * survives a GC means something in Kafka Streams still holds the raw bytes.
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
}
