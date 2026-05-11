package kafka.hw2.reporting;

public record ConsumerLogRecord(
        String messageId,
        String topic,
        String runId,
        int partition,
        long offset,
        String consumerId,
        long sentAtMs,
        long processedAtMs,
        int payloadSizeBytes
) {
}
