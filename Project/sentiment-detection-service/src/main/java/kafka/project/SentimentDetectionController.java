package kafka.project;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/sentiment")
public class SentimentDetectionController {

    private final SentimentDetectionProcessingService processingService;

    public SentimentDetectionController(SentimentDetectionProcessingService processingService) {
        this.processingService = processingService;
    }

    @PostMapping("/analyze")
    public SentimentResultMessage analyze(@RequestBody SentimentDetectionRequest request) {
        return processingService.analyze(request);
    }
}
