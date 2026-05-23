package kafka.project;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class LanguageDetectionListener {

    private static final Logger LOG = LoggerFactory.getLogger(LanguageDetectionListener.class);

    private final LanguageDetectionProcessingService processingService;
    private final KafkaTemplate<String, LanguageDetectedCommentMessage> kafkaTemplate;
    private final String outputTopic;

    public LanguageDetectionListener(
            LanguageDetectionProcessingService processingService,
            KafkaTemplate<String, LanguageDetectedCommentMessage> kafkaTemplate,
            @Value("${app.language-topic:reddit-comments-with-language}") String outputTopic
    ) {
        this.processingService = processingService;
        this.kafkaTemplate = kafkaTemplate;
        this.outputTopic = outputTopic;
    }

    @KafkaListener(topics = "${app.raw-topic:reddit-raw-comments}", groupId = "${spring.kafka.consumer.group-id}")
    public void onRawComment(RawCommentMessage message) {
        if (message == null || message.text() == null || message.text().isBlank()) {
            LOG.warn("Skipping empty raw comment message={}", message);
            return;
        }

        LanguageDetectedCommentMessage output = processingService.detect(message);
        kafkaTemplate.send(outputTopic, output.commentId(), output);
    }
}
