package kafka.hw2.producer;

public record ExperimentMessage(
        String messageId,
        String runId,
        String producerId,
        int shardIndex,
        int shardCount,
        String experimentName,
        long sentAtMs,
        int payloadSizeBytes,
        String payload
) {
}
