package com.example.flink.parquet;

import com.example.flink.model.WaveEvent;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class WaveEventParquetTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void writesAndReadsParquetRecord() throws Exception {
        WaveEvent event = new WaveEvent(
                "wave-1", "pack-1", "event-1", 42.5, "2026-11-08T12:00:00Z");
        GenericRecord record = new WaveEventAvroMapper().map(event);
        org.apache.hadoop.fs.Path output = new org.apache.hadoop.fs.Path(
                temporaryDirectory.resolve("events.parquet").toUri());
        Configuration configuration = new Configuration();

        try (var writer = AvroParquetWriter.<GenericRecord>builder(output)
                .withConf(configuration)
                .withSchema(WaveEventAvroMapper.schema())
                .build()) {
            writer.write(record);
        }

        try (var reader = AvroParquetReader.<GenericRecord>builder(output)
                .withConf(configuration)
                .build()) {
            GenericRecord restored = reader.read();

            assertNotNull(restored);
            assertEquals("wave-1", restored.get("wave_id").toString());
            assertEquals("pack-1", restored.get("pack_id").toString());
            assertEquals("event-1", restored.get("event_id").toString());
            assertEquals(42.5, restored.get("amount"));
            assertEquals("2026-11-08T12:00:00Z", restored.get("created_at").toString());
        }
    }
}
