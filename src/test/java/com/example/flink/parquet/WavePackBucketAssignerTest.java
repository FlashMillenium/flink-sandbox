package com.example.flink.parquet;

import com.example.flink.model.WaveEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WavePackBucketAssignerTest {
    private static final BucketAssigner.Context CONTEXT = new BucketAssigner.Context() {
        @Override
        public long currentProcessingTime() {
            return 0L;
        }

        @Override
        public long currentWatermark() {
            return 0L;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    };

    private final WavePackBucketAssigner assigner = new WavePackBucketAssigner();

    @Test
    void buildsWaveAndPackPath() {
        GenericRecord record = record("wave-1", "pack-1");

        String bucketId = assigner.getBucketId(record, CONTEXT);

        assertEquals("wave_id=wave-1/pack_id=pack-1", bucketId);
    }

    @Test
    void escapesPathSeparatorsAndUnicode() {
        GenericRecord record = record("wave/1", "пачка 1");

        String bucketId = assigner.getBucketId(record, CONTEXT);

        assertEquals("wave_id=wave%2F1/pack_id=%D0%BF%D0%B0%D1%87%D0%BA%D0%B0+1", bucketId);
    }

    @Test
    void serializesAndRestoresBucketId() throws Exception {
        SimpleVersionedSerializer<String> serializer = assigner.getSerializer();
        String bucketId = "wave_id=wave-1/pack_id=pack-1";

        byte[] serialized = serializer.serialize(bucketId);

        assertArrayEquals(bucketId.getBytes(StandardCharsets.UTF_8), serialized);
        assertEquals(bucketId, serializer.deserialize(1, serialized));
        assertThrows(IllegalArgumentException.class, () -> serializer.deserialize(2, serialized));
    }

    private static GenericRecord record(String waveId, String packId) {
        return new WaveEventAvroMapper().map(new WaveEvent(waveId, packId, null, 0.0, null));
    }
}
