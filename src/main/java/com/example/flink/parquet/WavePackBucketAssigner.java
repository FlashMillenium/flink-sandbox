package com.example.flink.parquet;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

public class WavePackBucketAssigner implements BucketAssigner<GenericRecord, String> {
    @Override
    public String getBucketId(GenericRecord record, Context context) {
        return "wave_id=" + encode(record.get("wave_id"))
                + "/pack_id=" + encode(record.get("pack_id"));
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return new SimpleVersionedSerializer<>() {
            @Override
            public int getVersion() {
                return 1;
            }

            @Override
            public byte[] serialize(String value) {
                return value.getBytes(StandardCharsets.UTF_8);
            }

            @Override
            public String deserialize(int version, byte[] serialized) {
                if (version != 1) {
                    throw new IllegalArgumentException("Unsupported bucket ID serializer version: " + version);
                }
                return new String(serialized, StandardCharsets.UTF_8);
            }
        };
    }

    private static String encode(Object value) {
        return URLEncoder.encode(value.toString(), StandardCharsets.UTF_8);
    }
}
