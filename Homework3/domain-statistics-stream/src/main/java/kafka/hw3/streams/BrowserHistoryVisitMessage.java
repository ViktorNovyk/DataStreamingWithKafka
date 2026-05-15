package kafka.hw3.streams;

public record BrowserHistoryVisitMessage(
        String messageId,
        String url,
        long visitCount,
        long sentAtMs
) {
}
