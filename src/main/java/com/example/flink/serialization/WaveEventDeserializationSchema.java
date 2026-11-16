package com.example.flink.serialization;

import com.example.flink.model.WaveEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class WaveEventDeserializationSchema implements DeserializationSchema<WaveEvent> {
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        objectMapper = new ObjectMapper();
    }

    @Override
    public WaveEvent deserialize(byte[] message) throws IOException {
        WaveEvent event = objectMapper.readValue(message, WaveEvent.class);
        event.validate();
        return event;
    }

    @Override
    public boolean isEndOfStream(WaveEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<WaveEvent> getProducedType() {
        return TypeInformation.of(WaveEvent.class);
    }
}
