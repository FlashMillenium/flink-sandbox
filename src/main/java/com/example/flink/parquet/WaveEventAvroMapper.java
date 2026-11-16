package com.example.flink.parquet;

import com.example.flink.model.WaveEvent;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.MapFunction;

public class WaveEventAvroMapper implements MapFunction<WaveEvent, GenericRecord> {
    private static final String SCHEMA_JSON = """
            {
              "type": "record",
              "name": "WaveEvent",
              "namespace": "com.example.flink.avro",
              "fields": [
                {"name": "wave_id", "type": "string"},
                {"name": "pack_id", "type": "string"},
                {"name": "event_id", "type": ["null", "string"], "default": null},
                {"name": "amount", "type": "double"},
                {"name": "created_at", "type": ["null", "string"], "default": null}
              ]
            }
            """;

    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    public static Schema schema() {
        return SCHEMA;
    }

    @Override
    public GenericRecord map(WaveEvent event) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("wave_id", event.getWaveId());
        record.put("pack_id", event.getPackId());
        record.put("event_id", event.getEventId());
        record.put("amount", event.getAmount());
        record.put("created_at", event.getCreatedAt());
        return record;
    }
}
