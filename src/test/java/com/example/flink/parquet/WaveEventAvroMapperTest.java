package com.example.flink.parquet;

import com.example.flink.model.WaveEvent;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class WaveEventAvroMapperTest {
    private final WaveEventAvroMapper mapper = new WaveEventAvroMapper();

    @Test
    void mapsAllFieldsToAvroRecord() {
        WaveEvent event = new WaveEvent(
                "wave-1", "pack-1", "event-1", 42.5, "2026-11-08T12:00:00Z");

        GenericRecord record = mapper.map(event);

        assertEquals("wave-1", record.get("wave_id").toString());
        assertEquals("pack-1", record.get("pack_id").toString());
        assertEquals("event-1", record.get("event_id").toString());
        assertEquals(42.5, record.get("amount"));
        assertEquals("2026-11-08T12:00:00Z", record.get("created_at").toString());
    }

    @Test
    void mapsNullableFieldsToNull() {
        WaveEvent event = new WaveEvent("wave-1", "pack-1", null, 0.0, null);

        GenericRecord record = mapper.map(event);

        assertNull(record.get("event_id"));
        assertNull(record.get("created_at"));
    }
}
