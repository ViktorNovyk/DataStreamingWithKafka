package kafka.hw2.producer;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import org.hibernate.validator.constraints.URL;

public record StartExperimentRequest(
        @NotBlank
        @Pattern(regexp = "[a-zA-Z0-9_-]+", message = "experimentName must match [a-zA-Z0-9_-]+")
        String experimentName,

        @NotBlank
        @URL(message = "csvUrl must be a valid URL")
        String csvUrl,

        String runId,
        Integer shardIndex,
        Integer shardCount
) {
}
