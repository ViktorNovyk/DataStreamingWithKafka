package kafka.project;

import java.util.List;

public record LanguageDetectionResponse(
        String commentId,
        String text,
        String language,
        double languageConfidence,
        boolean reliable,
        List<LanguageScore> candidates,
        long producedAtMs,
        long languageDetectedAtMs
) {
}
