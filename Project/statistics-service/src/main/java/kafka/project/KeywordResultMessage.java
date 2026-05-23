package kafka.project;

import java.util.List;

public record KeywordResultMessage(
        String commentId,
        String language,
        List<String> keywords,
        long keywordsExtractedAtMs
) {
}
