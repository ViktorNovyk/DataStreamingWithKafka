package kafka.project;

public record KeywordExtractionRequest(
        String commentId,
        String text,
        String language
) {
}
