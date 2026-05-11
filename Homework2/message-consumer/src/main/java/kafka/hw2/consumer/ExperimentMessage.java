package kafka.hw2.consumer;

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
