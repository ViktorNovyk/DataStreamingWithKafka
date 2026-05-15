package kafka.hw3.producer;

public record BrowserHistoryVisitMessage(
        String messageId,
        String url,
        long visitCount,
        long sentAtMs
) {
}
