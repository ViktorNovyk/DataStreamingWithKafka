package kafka.hw2.producer;

public record StartExperimentResponse(
        String experimentName,
        String topicName,
        String runId,
        int shardIndex,
        int shardCount,
        long producedMessages,
        long startedAtMs,
        long finishedAtMs,
        String status
) {
}
