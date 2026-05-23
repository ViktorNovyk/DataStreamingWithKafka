package kafka.project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class SentimentDetectionListener {

    private static final Logger LOG = LoggerFactory.getLogger(SentimentDetectionListener.class);

    private final SentimentDetectionProcessingService processingService;
    private final KafkaTemplate<String, SentimentResultMessage> kafkaTemplate;
    private final String outputTopic;

    public SentimentDetectionListener(
            SentimentDetectionProcessingService processingService,
            KafkaTemplate<String, SentimentResultMessage> kafkaTemplate,
            @Value("${app.sentiment-topic:reddit-comments-sentiment}") String outputTopic
    ) {
        this.processingService = processingService;
        this.kafkaTemplate = kafkaTemplate;
        this.outputTopic = outputTopic;
    }

    @KafkaListener(topics = "${app.language-topic:reddit-comments-with-language}", groupId = "${spring.kafka.consumer.group-id}")
    public void onLanguageDetectedComments(List<LanguageDetectedCommentMessage> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        List<SentimentResultMessage> outputs = processingService.analyzeBatch(messages);
        if (outputs.size() < messages.size()) {
            LOG.warn("Skipped {} empty language-detected messages in sentiment batch", messages.size() - outputs.size());
        }
        for (SentimentResultMessage output : outputs) {
            kafkaTemplate.send(outputTopic, output.commentId(), output);
        }
    }
}
