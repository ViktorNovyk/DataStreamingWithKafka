package kafka.project;

public record SentimentResultMessage(
        String commentId,
        String language,
        String sentiment,
        double confidence,
        double negativeScore,
        double neutralScore,
        double positiveScore,
        long sentimentDetectedAtMs
) {
}
