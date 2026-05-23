package kafka.project;

public record UploadRedditCsvResponse(
        String topic,
        long producedMessages,
        String status
) {
}
