package kafka.project;

public record SentimentDetectionRequest(
        String commentId,
        String text,
        String language
) {
}
