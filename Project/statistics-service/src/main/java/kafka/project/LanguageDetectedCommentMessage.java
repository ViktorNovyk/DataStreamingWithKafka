package kafka.project;

public record LanguageDetectedCommentMessage(
        String commentId,
        String text,
        String language,
        double languageConfidence,
        long producedAtMs,
        long languageDetectedAtMs
) {
}
