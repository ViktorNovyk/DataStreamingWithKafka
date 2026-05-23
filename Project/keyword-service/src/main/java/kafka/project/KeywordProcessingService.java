package kafka.project;

import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.List;

@Service
public class KeywordProcessingService {

    private final KeywordExtractor keywordExtractor;
    private final Clock clock;

    public KeywordProcessingService(KeywordExtractor keywordExtractor) {
        this.keywordExtractor = keywordExtractor;
        this.clock = Clock.systemUTC();
    }

    public KeywordResultMessage extract(LanguageDetectedCommentMessage message) {
        List<String> keywords = keywordExtractor.extract(message.text(), message.language());
        return new KeywordResultMessage(
                message.commentId(),
                message.language(),
                keywords,
                clock.millis()
        );
    }

    public KeywordResultMessage extract(KeywordExtractionRequest request) {
        LanguageDetectedCommentMessage message = new LanguageDetectedCommentMessage(
                request.commentId(),
                request.text(),
                request.language(),
                0.0,
                clock.millis(),
                clock.millis()
        );
        return extract(message);
    }
}
