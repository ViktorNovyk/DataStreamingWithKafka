package kafka.project;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/language")
public class LanguageDetectionController {

    private final LanguageDetectionProcessingService processingService;

    public LanguageDetectionController(LanguageDetectionProcessingService processingService) {
        this.processingService = processingService;
    }

    @PostMapping("/detect")
    public LanguageDetectionResponse detect(@RequestBody LanguageDetectionRequest request) {
        return processingService.detectForApi(request);
    }
}
