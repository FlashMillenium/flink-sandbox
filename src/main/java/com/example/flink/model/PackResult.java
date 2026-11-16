package com.example.flink.model;

import java.io.Serializable;

public class PackResult implements Serializable {
    private String waveId;
    private String packId;
    private long processedCount;
    private String completedAt;

    public PackResult() {
    }

    public PackResult(String waveId, String packId, long processedCount, String completedAt) {
        this.waveId = waveId;
        this.packId = packId;
        this.processedCount = processedCount;
        this.completedAt = completedAt;
    }

    public String getWaveId() {
        return waveId;
    }

    public void setWaveId(String waveId) {
        this.waveId = waveId;
    }

    public String getPackId() {
        return packId;
    }

    public void setPackId(String packId) {
        this.packId = packId;
    }

    public long getProcessedCount() {
        return processedCount;
    }

    public void setProcessedCount(long processedCount) {
        this.processedCount = processedCount;
    }

    public String getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(String completedAt) {
        this.completedAt = completedAt;
    }

    @Override
    public String toString() {
        return "PackResult{" +
                "waveId='" + waveId + '\'' +
                ", packId='" + packId + '\'' +
                ", processedCount=" + processedCount +
                ", completedAt='" + completedAt + '\'' +
                '}';
    }
}
