package com.example.flink.serialization;

import com.example.flink.model.WaveEvent;
import com.fasterxml.jackson.core.JsonParseException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WaveEventDeserializationSchemaTest {
    private WaveEventDeserializationSchema deserializer;

    @BeforeEach
    void setUp() {
        deserializer = new WaveEventDeserializationSchema();
        deserializer.open(null);
    }

    @Test
    void deserializesAllFields() throws Exception {
        WaveEvent event = deserialize("""
                {
                  "wave_id": "wave-1",
                  "pack_id": "pack-1",
                  "event_id": "event-1",
                  "amount": 42.5,
                  "created_at": "2026-11-08T12:00:00Z"
                }
                """);

        assertEquals("wave-1", event.getWaveId());
        assertEquals("pack-1", event.getPackId());
        assertEquals("event-1", event.getEventId());
        assertEquals(42.5, event.getAmount());
        assertEquals("2026-11-08T12:00:00Z", event.getCreatedAt());
    }

    @Test
    void ignoresAdditionalJsonFields() throws Exception {
        WaveEvent event = deserialize("""
                {"wave_id":"wave-1","pack_id":"pack-1","source":"generator"}
                """);

        assertEquals("wave-1", event.getWaveId());
        assertEquals("pack-1", event.getPackId());
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "{\"pack_id\":\"pack-1\"}",
            "{\"wave_id\":\"\",\"pack_id\":\"pack-1\"}",
            "{\"wave_id\":\"wave-1\"}",
            "{\"wave_id\":\"wave-1\",\"pack_id\":null}",
            "{\"wave_id\":\"wave-1\",\"pack_id\":\"  \"}"
    })
    void rejectsMissingOrBlankRequiredFields(String json) {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> deserialize(json));

        org.junit.jupiter.api.Assertions.assertTrue(error.getMessage().contains("wave_id")
                || error.getMessage().contains("pack_id"));
    }

    @Test
    void rejectsMalformedJson() {
        assertThrows(JsonParseException.class, () -> deserialize("{not-json}"));
    }

    private WaveEvent deserialize(String json) throws Exception {
        return deserializer.deserialize(json.getBytes(StandardCharsets.UTF_8));
    }
}
