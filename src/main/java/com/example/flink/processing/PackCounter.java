package com.example.flink.processing;

import com.example.flink.model.PackResult;
import com.example.flink.model.WaveEvent;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.util.Objects;

public class PackCounter extends KeyedProcessFunction<Tuple2<String, String>, WaveEvent, PackResult> {
    private final long inactivityTimeoutMs;

    private transient ValueState<Long> countState;
    private transient ValueState<Long> timerState;

    public PackCounter(long inactivityTimeoutMs) {
        if (inactivityTimeoutMs <= 0) {
            throw new IllegalArgumentException("Inactivity timeout must be greater than zero");
        }
        this.inactivityTimeoutMs = inactivityTimeoutMs;
    }

    @Override
    public void open(OpenContext openContext) {
        countState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("processed-count", Types.LONG));
        timerState = getRuntimeContext().getState(
                new ValueStateDescriptor<>("inactivity-timer", Types.LONG));
    }

    @Override
    public void processElement(
            WaveEvent event,
            KeyedProcessFunction<Tuple2<String, String>, WaveEvent, PackResult>.Context context,
            Collector<PackResult> out) throws Exception {
        long now = context.timerService().currentProcessingTime();
        Long previousTimer = timerState.value();
        Long previousCount = countState.value();

        if (previousTimer != null && previousTimer <= now && previousCount != null) {
            emitResult(context.getCurrentKey(), previousCount, now, out);
            previousCount = null;
        } else if (previousTimer != null) {
            context.timerService().deleteProcessingTimeTimer(previousTimer);
        }

        countState.update(previousCount == null ? 1L : previousCount + 1);
        long nextTimer = now + inactivityTimeoutMs;
        timerState.update(nextTimer);
        context.timerService().registerProcessingTimeTimer(nextTimer);
    }

    @Override
    public void onTimer(
            long timestamp,
            KeyedProcessFunction<Tuple2<String, String>, WaveEvent, PackResult>.OnTimerContext context,
            Collector<PackResult> out) throws Exception {
        Long scheduledTimer = timerState.value();
        if (!Objects.equals(scheduledTimer, timestamp)) {
            return;
        }

        Long count = countState.value();
        if (count != null) {
            emitResult(context.getCurrentKey(), count, timestamp, out);
        }
        countState.clear();
        timerState.clear();
    }

    private static void emitResult(
            Tuple2<String, String> key,
            long count,
            long completedAtMs,
            Collector<PackResult> out) {
        out.collect(new PackResult(
                key.f0,
                key.f1,
                count,
                Instant.ofEpochMilli(completedAtMs).toString()));
    }
}
