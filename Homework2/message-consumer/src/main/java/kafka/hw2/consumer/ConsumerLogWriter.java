package kafka.hw2.consumer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Component
public class ConsumerLogWriter {

    private static final String HEADER = "message_id,topic,run_id,partition,offset,consumer_id,sent_at_ms,processed_at_ms,payload_size_bytes";
    private final Path logsRoot;
    private final ConcurrentMap<Path, Object> fileLocks = new ConcurrentHashMap<>();

    public ConsumerLogWriter(@Value("${app.logs-root:./logs}") String logsRootPath) {
        this.logsRoot = Path.of(logsRootPath);
    }

    public void write(ConsumerLogRow row) {
        Path targetFile = logsRoot
                .resolve(row.topic())
                .resolve(row.runId())
                .resolve("consumer-" + row.consumerId() + ".csv");

        Object lock = fileLocks.computeIfAbsent(targetFile, ignored -> new Object());
        synchronized (lock) {
            try {
                Files.createDirectories(targetFile.getParent());
                boolean fileExists = Files.exists(targetFile);
                try (BufferedWriter writer = Files.newBufferedWriter(
                        targetFile,
                        StandardCharsets.UTF_8,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.APPEND
                )) {
                    if (!fileExists) {
                        writer.write(HEADER);
                        writer.newLine();
                    }
                    writer.write(toCsvRow(row));
                    writer.newLine();
                }
            } catch (IOException e) {
                throw new IllegalStateException("Failed to write consumer log to " + targetFile, e);
            }
        }
    }

    private String toCsvRow(ConsumerLogRow row) {
        return String.join(",",
                row.messageId(),
                row.topic(),
                row.runId(),
                Integer.toString(row.partition()),
                Long.toString(row.offset()),
                row.consumerId(),
                Long.toString(row.sentAtMs()),
                Long.toString(row.processedAtMs()),
                Integer.toString(row.payloadSizeBytes())
        );
    }
}
