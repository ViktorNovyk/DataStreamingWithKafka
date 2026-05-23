package kafka.project;

import org.springframework.stereotype.Service;

import java.time.Clock;

@Service
public class LanguageDetectionProcessingService {

    private final LanguageDetectorService languageDetectorService;
    private final Clock clock;

    public LanguageDetectionProcessingService(LanguageDetectorService languageDetectorService) {
        this.languageDetectorService = languageDetectorService;
        this.clock = Clock.systemUTC();
    }

    public LanguageDetectedCommentMessage detect(RawCommentMessage message) {
        LanguagePrediction prediction = languageDetectorService.detect(message.text());
        return new LanguageDetectedCommentMessage(
                message.commentId(),
                message.text(),
                prediction.language(),
                prediction.confidence(),
                message.producedAtMs(),
                clock.millis()
        );
    }

    public LanguageDetectionResponse detectForApi(LanguageDetectionRequest request) {
        long now = clock.millis();
        String commentId = request.commentId();
        long producedAtMs = request.producedAtMs() == null ? now : request.producedAtMs();
        LanguagePrediction prediction = languageDetectorService.detect(request.text());
        return new LanguageDetectionResponse(
                commentId,
                request.text(),
                prediction.language(),
                prediction.confidence(),
                prediction.reliable(),
                prediction.candidates(),
                producedAtMs,
                clock.millis()
        );
    }
}
