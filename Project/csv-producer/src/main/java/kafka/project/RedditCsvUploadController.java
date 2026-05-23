package kafka.project;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping("/api/v1/reddit")
public class RedditCsvUploadController {

    private final RedditCsvProducerService redditCsvProducerService;

    public RedditCsvUploadController(RedditCsvProducerService redditCsvProducerService) {
        this.redditCsvProducerService = redditCsvProducerService;
    }

    @PostMapping("/upload")
    @ResponseStatus(HttpStatus.ACCEPTED)
    public UploadRedditCsvResponse upload(@RequestParam("file") MultipartFile file) {
        return redditCsvProducerService.produce(file);
    }
}
