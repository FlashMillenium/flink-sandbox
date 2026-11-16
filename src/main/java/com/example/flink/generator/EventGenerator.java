package com.example.flink.generator;

import com.example.flink.model.WaveEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class EventGenerator {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        GeneratorConfig config = GeneratorConfig.parse(args);
        if (config.help()) {
            printHelp();
            return;
        }

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Random random = new Random();
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
            for (long index = 0; config.count() == 0 || index < config.count(); index++) {
                String waveId = "wave-" + (index % config.waves());
                String packId = "pack-" + ((index / config.waves()) % config.packs());
                WaveEvent event = new WaveEvent(
                        waveId,
                        packId,
                        UUID.randomUUID().toString(),
                        random.nextDouble(1.0, 1_000.0),
                        Instant.now().toString());

                String key = waveId + ":" + packId;
                String value = OBJECT_MAPPER.writeValueAsString(event);
                producer.send(new ProducerRecord<>(config.topic(), key, value)).get();
                System.out.println("Sent: " + value);

                if (config.intervalMs() > 0) {
                    Thread.sleep(config.intervalMs());
                }
            }
            producer.flush();
        }
    }

    private static void printHelp() {
        System.out.println("""
                Usage: java -cp target/flink-sandbox-1.0.0.jar \
                com.example.flink.generator.EventGenerator [options]

                Options:
                  --bootstrap-servers <host:port>  Kafka address (default: localhost:29092)
                  --topic <name>                  Target topic (default: input_topic)
                  --count <number>                Number of events; 0 means unlimited (default: 100)
                  --interval-ms <number>          Delay between events (default: 100)
                  --waves <number>                Number of wave IDs (default: 2)
                  --packs <number>                Number of pack IDs per wave (default: 3)
                """);
    }

    private record GeneratorConfig(
            String bootstrapServers,
            String topic,
            long count,
            long intervalMs,
            int waves,
            int packs,
            boolean help) {

        static GeneratorConfig parse(String[] args) {
            Map<String, String> options = new HashMap<>();
            for (int index = 0; index < args.length; index++) {
                if (args[index].equals("--help") || args[index].equals("-h")) {
                    return new GeneratorConfig("", "", 0, 0, 1, 1, true);
                }
                if (!args[index].startsWith("--") || index + 1 >= args.length) {
                    throw new IllegalArgumentException("Expected --option value, got: " + args[index]);
                }
                options.put(args[index], args[++index]);
            }

            GeneratorConfig config = new GeneratorConfig(
                    options.getOrDefault("--bootstrap-servers",
                            environment("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")),
                    options.getOrDefault("--topic", environment("KAFKA_TOPIC", "input_topic")),
                    nonNegativeLong("--count", options.getOrDefault("--count", "100")),
                    nonNegativeLong("--interval-ms", options.getOrDefault("--interval-ms", "100")),
                    positiveInt("--waves", options.getOrDefault("--waves", "2")),
                    positiveInt("--packs", options.getOrDefault("--packs", "3")),
                    false);
            if (options.keySet().stream().anyMatch(key -> !SetOfOptions.ALL.contains(key))) {
                throw new IllegalArgumentException("Unknown option. Use --help to list supported options");
            }
            return config;
        }

        private static String environment(String name, String defaultValue) {
            String value = System.getenv(name);
            return value == null || value.isBlank() ? defaultValue : value;
        }

        private static long nonNegativeLong(String name, String value) {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new IllegalArgumentException(name + " must not be negative");
            }
            return parsed;
        }

        private static int positiveInt(String name, String value) {
            int parsed = Integer.parseInt(value);
            if (parsed <= 0) {
                throw new IllegalArgumentException(name + " must be greater than zero");
            }
            return parsed;
        }
    }

    private static final class SetOfOptions {
        private static final java.util.Set<String> ALL = java.util.Set.of(
                "--bootstrap-servers", "--topic", "--count", "--interval-ms", "--waves", "--packs");
    }
}
