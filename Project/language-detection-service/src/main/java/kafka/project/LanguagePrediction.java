package kafka.project;

import java.util.List;

public record LanguagePrediction(
        String language,
        double confidence,
        boolean reliable,
        List<LanguageScore> candidates
) {
}
