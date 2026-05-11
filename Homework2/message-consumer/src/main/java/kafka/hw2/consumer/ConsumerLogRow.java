package kafka.hw2.consumer;

public record ConsumerLogRow(
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
