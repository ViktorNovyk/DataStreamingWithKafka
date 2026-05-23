package kafka.project;

public record RawCommentMessage(
        String commentId,
        String text,
        long producedAtMs
) {
}
