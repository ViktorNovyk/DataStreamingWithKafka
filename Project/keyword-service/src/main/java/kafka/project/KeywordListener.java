package kafka.project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KeywordListener {

    private static final Logger LOG = LoggerFactory.getLogger(KeywordListener.class);

    private final KeywordProcessingService processingService;
    private final KafkaTemplate<String, KeywordResultMessage> kafkaTemplate;
    private final String outputTopic;

    public KeywordListener(
            KeywordProcessingService processingService,
            KafkaTemplate<String, KeywordResultMessage> kafkaTemplate,
            @Value("${app.keyword-topic:reddit-comments-keywords}") String outputTopic
    ) {
        this.processingService = processingService;
        this.kafkaTemplate = kafkaTemplate;
        this.outputTopic = outputTopic;
    }

    @KafkaListener(topics = "${app.language-topic:reddit-comments-with-language}", groupId = "${spring.kafka.consumer.group-id}")
    public void onLanguageDetectedComment(LanguageDetectedCommentMessage message) {
        if (message == null || message.text() == null || message.text().isBlank()) {
            LOG.warn("Skipping empty language-detected comment message={}", message);
            return;
        }

        KeywordResultMessage output = processingService.extract(message);
        kafkaTemplate.send(outputTopic, output.commentId(), output);
    }
}
