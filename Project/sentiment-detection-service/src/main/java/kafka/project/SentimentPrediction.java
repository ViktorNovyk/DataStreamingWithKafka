package kafka.project;

public record SentimentPrediction(
        String sentiment,
        double confidence,
        double negativeScore,
        double neutralScore,
        double positiveScore
) {
}
