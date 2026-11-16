package com.example.flink.config;

import java.time.Duration;

public record JobConfig(
        String kafkaBootstrapServers,
        String kafkaTopic,
        String kafkaGroupId,
        String s3OutputPath,
        long checkpointIntervalMs,
        long packInactivityTimeoutMs) {

    public static JobConfig fromEnvironment() {
        return new JobConfig(
                environment("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
                environment("KAFKA_TOPIC", "input_topic"),
                environment("KAFKA_GROUP_ID", "flink-wave-pack-counter"),
                environment("S3_OUTPUT_PATH", "s3://flink-data/events"),
                positiveLong("CHECKPOINT_INTERVAL_MS", 10_000),
                positiveLong("PACK_INACTIVITY_TIMEOUT_MS", Duration.ofMinutes(5).toMillis()));
    }

    private static String environment(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static long positiveLong(String name, long defaultValue) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        long parsed = Long.parseLong(value);
        if (parsed <= 0) {
            throw new IllegalArgumentException(name + " must be greater than zero");
        }
        return parsed;
    }
}
