package kafka.project;

import org.springframework.stereotype.Service;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;

@Service
public class SentimentDetectionProcessingService {

    private final OnnxSentimentAnalyzer sentimentAnalyzer;
    private final Clock clock;

    public SentimentDetectionProcessingService(OnnxSentimentAnalyzer sentimentAnalyzer) {
        this.sentimentAnalyzer = sentimentAnalyzer;
        this.clock = Clock.systemUTC();
    }

    public SentimentResultMessage analyze(LanguageDetectedCommentMessage message) {
        SentimentPrediction prediction = sentimentAnalyzer.analyze(message.text());
        return toResult(message, prediction);
    }

    public List<SentimentResultMessage> analyzeBatch(List<LanguageDetectedCommentMessage> messages) {
        if (messages.isEmpty()) {
            return List.of();
        }

        List<String> texts = messages.stream()
                .map(LanguageDetectedCommentMessage::text)
                .toList();
        List<SentimentPrediction> predictions = sentimentAnalyzer.analyzeBatch(texts);
        List<SentimentResultMessage> results = new ArrayList<>(messages.size());
        for (int i = 0; i < messages.size(); i++) {
            results.add(toResult(messages.get(i), predictions.get(i)));
        }
        return results;
    }

    public SentimentResultMessage analyze(SentimentDetectionRequest request) {
        LanguageDetectedCommentMessage message = new LanguageDetectedCommentMessage(
                request.commentId(),
                request.text(),
                request.language(),
                0.0,
                clock.millis(),
                clock.millis()
        );
        return analyze(message);
    }

    private SentimentResultMessage toResult(LanguageDetectedCommentMessage message, SentimentPrediction prediction) {
        return new SentimentResultMessage(
                message.commentId(),
                message.language(),
                prediction.sentiment(),
                prediction.confidence(),
                prediction.negativeScore(),
                prediction.neutralScore(),
                prediction.positiveScore(),
                clock.millis()
        );
    }

}
