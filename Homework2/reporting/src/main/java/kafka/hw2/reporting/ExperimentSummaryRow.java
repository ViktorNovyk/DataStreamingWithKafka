package kafka.hw2.reporting;

public record ExperimentSummaryRow(
        String topic,
        String runId,
        int messagesUnique,
        long totalPayloadBytes,
        double durationSec,
        double throughputMbps,
        long maxLatencyMs
) {
}
