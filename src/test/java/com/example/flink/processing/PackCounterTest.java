package com.example.flink.processing;

import com.example.flink.model.PackResult;
import com.example.flink.model.WaveEvent;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PackCounterTest {
    private static final long TIMEOUT_MS = Duration.ofMinutes(5).toMillis();

    private KeyedOneInputStreamOperatorTestHarness<Tuple2<String, String>, WaveEvent, PackResult> harness;

    @BeforeEach
    void setUp() throws Exception {
        KeyedProcessOperator<Tuple2<String, String>, WaveEvent, PackResult> operator =
                new KeyedProcessOperator<>(new PackCounter(TIMEOUT_MS));
        harness = new KeyedOneInputStreamOperatorTestHarness<>(
                operator,
                event -> Tuple2.of(event.getWaveId(), event.getPackId()),
                Types.TUPLE(Types.STRING, Types.STRING));
        harness.open();
    }

    @AfterEach
    void tearDown() throws Exception {
        harness.close();
    }

    @Test
    void emitsIndependentCountsAfterFiveMinutes() throws Exception {
        harness.setProcessingTime(1_000L);
        process("wave-1", "pack-1");
        process("wave-1", "pack-1");
        process("wave-1", "pack-2");
        process("wave-2", "pack-1");

        harness.setProcessingTime(1_000L + TIMEOUT_MS - 1);
        assertTrue(harness.extractOutputStreamRecords().isEmpty());

        harness.setProcessingTime(1_000L + TIMEOUT_MS);

        Map<String, PackResult> results = resultsByWaveAndPack();
        assertEquals(3, results.size());
        assertEquals(2, results.get("wave-1/pack-1").getProcessedCount());
        assertEquals(1, results.get("wave-1/pack-2").getProcessedCount());
        assertEquals(1, results.get("wave-2/pack-1").getProcessedCount());
        assertEquals(
                Instant.ofEpochMilli(1_000L + TIMEOUT_MS).toString(),
                results.get("wave-1/pack-1").getCompletedAt());
    }

    @Test
    void newEventResetsInactivityTimer() throws Exception {
        harness.setProcessingTime(1_000L);
        process("wave-1", "pack-1");

        harness.setProcessingTime(1_000L + TIMEOUT_MS - 1_000L);
        process("wave-1", "pack-1");

        harness.setProcessingTime(1_000L + TIMEOUT_MS);
        assertTrue(harness.extractOutputStreamRecords().isEmpty());

        harness.setProcessingTime(2 * TIMEOUT_MS);

        List<StreamRecord<? extends PackResult>> output = harness.extractOutputStreamRecords();
        assertEquals(1, output.size());
        assertEquals(2, output.getFirst().getValue().getProcessedCount());
    }

    @Test
    void startsANewBatchAfterCompletion() throws Exception {
        harness.setProcessingTime(1_000L);
        process("wave-1", "pack-1");
        process("wave-1", "pack-1");
        harness.setProcessingTime(1_000L + TIMEOUT_MS);
        assertEquals(2, harness.extractOutputStreamRecords().getFirst().getValue().getProcessedCount());
        harness.getOutput().clear();

        harness.setProcessingTime(2_000L + TIMEOUT_MS);
        process("wave-1", "pack-1");
        harness.setProcessingTime(2_000L + 2 * TIMEOUT_MS);

        List<StreamRecord<? extends PackResult>> output = harness.extractOutputStreamRecords();
        assertEquals(1, output.size());
        assertEquals(1, output.getFirst().getValue().getProcessedCount());
    }

    @Test
    void rejectsNonPositiveTimeout() {
        assertThrows(IllegalArgumentException.class, () -> new PackCounter(0));
    }

    private void process(String waveId, String packId) throws Exception {
        harness.processElement(new StreamRecord<>(
                new WaveEvent(waveId, packId, null, 0.0, null)));
    }

    private Map<String, PackResult> resultsByWaveAndPack() {
        return harness.extractOutputStreamRecords().stream()
                .map(StreamRecord::getValue)
                .collect(Collectors.toMap(
                        result -> result.getWaveId() + "/" + result.getPackId(),
                        Function.identity()));
    }
}
