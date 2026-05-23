package kafka.project;

public record LanguageScore(
        String language,
        String originalLanguage,
        double confidence
) {
}
