package kafka.hw3.producer;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/v1/history")
public class HistoryUploadController {

    private final HistoryProducerService historyProducerService;

    public HistoryUploadController(HistoryProducerService historyProducerService) {
        this.historyProducerService = historyProducerService;
    }

    @PostMapping("/upload")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public UploadHistoryResponse upload(@RequestParam("file") MultipartFile file) {
        return historyProducerService.produce(file);
    }
}
