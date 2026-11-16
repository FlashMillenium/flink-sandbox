package com.example.flink;

import com.example.flink.config.JobConfig;
import com.example.flink.model.PackResult;
import com.example.flink.model.WaveEvent;
import com.example.flink.parquet.WaveEventAvroMapper;
import com.example.flink.parquet.WavePackBucketAssigner;
import com.example.flink.processing.PackCounter;
import com.example.flink.serialization.WaveEventDeserializationSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;

public class WavePackJob {
    public static void main(String[] args) throws Exception {
        JobConfig config = JobConfig.fromEnvironment();
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(config.checkpointIntervalMs(), CheckpointingMode.EXACTLY_ONCE);

        KafkaSource<WaveEvent> source = KafkaSource.<WaveEvent>builder()
                .setBootstrapServers(config.kafkaBootstrapServers())
                .setTopics(config.kafkaTopic())
                .setGroupId(config.kafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new WaveEventDeserializationSchema())
                .build();

        DataStream<WaveEvent> events = environment.fromSource(
                        source,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka input")
                .uid("kafka-source");

        events.map(new WaveEventAvroMapper())
                .returns(new GenericRecordAvroTypeInfo(WaveEventAvroMapper.schema()))
                .name("Map to Avro")
                .sinkTo(parquetSink(config.s3OutputPath()))
                .name("Parquet S3 sink")
                .uid("parquet-s3-sink");

        DataStream<PackResult> completedPacks = events
                .keyBy(event -> Tuple2.of(event.getWaveId(), event.getPackId()))
                .process(new PackCounter(config.packInactivityTimeoutMs()))
                .name("Count packs after inactivity")
                .uid("pack-counter");

        completedPacks.print("Completed pack");

        environment.execute("Kafka JSON to S3 Parquet with wave/pack counters");
    }

    private static FileSink<GenericRecord> parquetSink(String outputPath) {
        return FileSink.forBulkFormat(
                        new Path(outputPath),
                        AvroParquetWriters.forGenericRecord(WaveEventAvroMapper.schema()))
                .withBucketAssigner(new WavePackBucketAssigner())
                .withRollingPolicy(OnCheckpointRollingPolicy.build())
                .withOutputFileConfig(OutputFileConfig.builder()
                        .withPartPrefix("events")
                        .withPartSuffix(".parquet")
                        .build())
                .build();
    }
}
