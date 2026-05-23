package kafka.project;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/keywords")
public class KeywordController {

    private final KeywordProcessingService processingService;

    public KeywordController(KeywordProcessingService processingService) {
        this.processingService = processingService;
    }

    @PostMapping("/extract")
    public KeywordResultMessage extract(@RequestBody KeywordExtractionRequest request) {
        return processingService.extract(request);
    }
}
