package com.example.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class WaveEvent implements Serializable {
    private String waveId;
    private String packId;
    private String eventId;
    private double amount;
    private String createdAt;

    public WaveEvent() {
    }

    public WaveEvent(String waveId, String packId, String eventId, double amount, String createdAt) {
        this.waveId = waveId;
        this.packId = packId;
        this.eventId = eventId;
        this.amount = amount;
        this.createdAt = createdAt;
    }

    @JsonProperty("wave_id")
    public String getWaveId() {
        return waveId;
    }

    @JsonProperty("wave_id")
    public void setWaveId(String waveId) {
        this.waveId = waveId;
    }

    @JsonProperty("pack_id")
    public String getPackId() {
        return packId;
    }

    @JsonProperty("pack_id")
    public void setPackId(String packId) {
        this.packId = packId;
    }

    @JsonProperty("event_id")
    public String getEventId() {
        return eventId;
    }

    @JsonProperty("event_id")
    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    @JsonProperty("created_at")
    public String getCreatedAt() {
        return createdAt;
    }

    @JsonProperty("created_at")
    public void setCreatedAt(String createdAt) {
        this.createdAt = createdAt;
    }

    public void validate() {
        if (waveId == null || waveId.isBlank()) {
            throw new IllegalArgumentException("JSON object must contain a non-empty string field 'wave_id'");
        }
        if (packId == null || packId.isBlank()) {
            throw new IllegalArgumentException("JSON object must contain a non-empty string field 'pack_id'");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof WaveEvent that)) {
            return false;
        }
        return Double.compare(amount, that.amount) == 0
                && Objects.equals(waveId, that.waveId)
                && Objects.equals(packId, that.packId)
                && Objects.equals(eventId, that.eventId)
                && Objects.equals(createdAt, that.createdAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(waveId, packId, eventId, amount, createdAt);
    }

    @Override
    public String toString() {
        return "WaveEvent{" +
                "waveId='" + waveId + '\'' +
                ", packId='" + packId + '\'' +
                ", eventId='" + eventId + '\'' +
                ", amount=" + amount +
                ", createdAt='" + createdAt + '\'' +
                '}';
    }
}
