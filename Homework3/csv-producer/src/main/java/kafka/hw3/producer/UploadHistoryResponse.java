package kafka.hw3.producer;

public record UploadHistoryResponse(
        String topic,
        long producedMessages,
        String status
) {
}
