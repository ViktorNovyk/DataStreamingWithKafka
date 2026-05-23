package kafka.project;

public record LanguageDetectionRequest(
        String commentId,
        String text,
        Long producedAtMs
) {
}
