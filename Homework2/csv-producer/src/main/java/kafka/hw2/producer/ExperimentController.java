package kafka.hw2.producer;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/experiments")
public class ExperimentController {

    private final ExperimentProducerService experimentProducerService;

    public ExperimentController(ExperimentProducerService experimentProducerService) {
        this.experimentProducerService = experimentProducerService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.ACCEPTED)
    public StartExperimentResponse startExperiment(@Valid @RequestBody StartExperimentRequest request) {
        return experimentProducerService.start(request);
    }
}
