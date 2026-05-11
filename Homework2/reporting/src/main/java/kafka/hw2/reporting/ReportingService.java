package kafka.hw2.reporting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

@Service
public class ReportingService {

    private static final Logger LOG = LoggerFactory.getLogger(ReportingService.class);
    private static final String SUMMARY_HEADER = "topic,run_id,messages_unique,total_payload_bytes,duration_sec,throughput_mbps,max_latency_ms";

    private final Path logsRoot;
    private final Path reportsRoot;
    private final String reportFileName;
    private final Clock clock;

    public ReportingService(
            @Value("${app.logs-root:./logs}") String logsRoot,
            @Value("${app.reports-root:./reports}") String reportsRoot,
            @Value("${app.report-file-name:summary.csv}") String reportFileName
    ) {
        this.logsRoot = Path.of(logsRoot);
        this.reportsRoot = Path.of(reportsRoot);
        this.reportFileName = reportFileName;
        this.clock = Clock.systemUTC();
    }

    public synchronized ReportGenerationResponse generateReport() {
        List<ExperimentSummaryRow> rows = generateSummary();
        printSummary(rows);
        Path reportFile = writeSummary(rows);
        return new ReportGenerationResponse(
                clock.millis(),
                reportFile.toAbsolutePath().toString(),
                rows.size(),
                rows
        );
    }

    public List<ExperimentSummaryRow> generateSummary() {
        if (!Files.exists(logsRoot)) {
            LOG.warn("Logs root {} does not exist. Summary will be empty.", logsRoot.toAbsolutePath());
            return List.of();
        }

        Map<ExperimentRunKey, Map<String, ConsumerLogRecord>> groupedRecords = new HashMap<>();
        try (Stream<Path> paths = Files.walk(logsRoot)) {
            paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().startsWith("consumer-"))
                    .filter(path -> path.getFileName().toString().endsWith(".csv"))
                    .forEach(path -> readConsumerLog(path, groupedRecords));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to traverse logs directory " + logsRoot.toAbsolutePath(), e);
        }

        List<ExperimentSummaryRow> result = new ArrayList<>();
        for (Map.Entry<ExperimentRunKey, Map<String, ConsumerLogRecord>> entry : groupedRecords.entrySet()) {
            result.add(toSummaryRow(entry.getKey(), entry.getValue().values()));
        }
        result.sort(Comparator
                .comparing(ExperimentSummaryRow::topic)
                .thenComparing(ExperimentSummaryRow::runId));
        return result;
    }

    private void readConsumerLog(Path file, Map<ExperimentRunKey, Map<String, ConsumerLogRecord>> groupedRecords) {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            String line = reader.readLine(); // header
            if (line == null) {
                return;
            }
            while ((line = reader.readLine()) != null) {
                if (line.isBlank()) {
                    continue;
                }
                ConsumerLogRecord record = parseRecord(line, file);
                ExperimentRunKey key = new ExperimentRunKey(record.topic(), record.runId());
                groupedRecords
                        .computeIfAbsent(key, ignored -> new HashMap<>())
                        .merge(record.messageId(), record, this::pickEarlierProcessedRecord);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read consumer log " + file.toAbsolutePath(), e);
        }
    }

    private ConsumerLogRecord pickEarlierProcessedRecord(ConsumerLogRecord first, ConsumerLogRecord second) {
        return first.processedAtMs() <= second.processedAtMs() ? first : second;
    }

    private ConsumerLogRecord parseRecord(String line, Path sourceFile) {
        String[] parts = line.split(",", -1);
        if (parts.length != 9) {
            throw new IllegalStateException("Invalid record in " + sourceFile.toAbsolutePath() + ": " + line);
        }
        try {
            return new ConsumerLogRecord(
                    parts[0],
                    parts[1],
                    parts[2],
                    Integer.parseInt(parts[3]),
                    Long.parseLong(parts[4]),
                    parts[5],
                    Long.parseLong(parts[6]),
                    Long.parseLong(parts[7]),
                    Integer.parseInt(parts[8])
            );
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid numeric field in " + sourceFile.toAbsolutePath() + ": " + line, e);
        }
    }

    private ExperimentSummaryRow toSummaryRow(ExperimentRunKey key, Iterable<ConsumerLogRecord> records) {
        int messagesUnique = 0;
        long totalPayloadBytes = 0;
        long minSentAt = Long.MAX_VALUE;
        long maxProcessedAt = Long.MIN_VALUE;
        long maxLatency = Long.MIN_VALUE;

        for (ConsumerLogRecord record : records) {
            messagesUnique++;
            totalPayloadBytes += record.payloadSizeBytes();
            minSentAt = Math.min(minSentAt, record.sentAtMs());
            maxProcessedAt = Math.max(maxProcessedAt, record.processedAtMs());
            maxLatency = Math.max(maxLatency, record.processedAtMs() - record.sentAtMs());
        }

        if (messagesUnique == 0) {
            return new ExperimentSummaryRow(key.topic(), key.runId(), 0, 0, 0.0, 0.0, 0);
        }

        double durationSec = Math.max((maxProcessedAt - minSentAt) / 1000.0, 0.001);
        double throughputMbps = ((totalPayloadBytes * 8.0) / durationSec) / 1_000_000.0;

        return new ExperimentSummaryRow(
                key.topic(),
                key.runId(),
                messagesUnique,
                totalPayloadBytes,
                durationSec,
                throughputMbps,
                maxLatency
        );
    }

    private void printSummary(List<ExperimentSummaryRow> rows) {
        if (rows.isEmpty()) {
            LOG.info("No experiment logs found in {}", logsRoot.toAbsolutePath());
            return;
        }
        LOG.info("Report summary ({} rows):", rows.size());
        for (ExperimentSummaryRow row : rows) {
            LOG.info(
                    "topic={} runId={} messages={} bytes={} durationSec={} throughputMbps={} maxLatencyMs={}",
                    row.topic(),
                    row.runId(),
                    row.messagesUnique(),
                    row.totalPayloadBytes(),
                    round3(row.durationSec()),
                    round6(row.throughputMbps()),
                    row.maxLatencyMs()
            );
        }
    }

    private Path writeSummary(List<ExperimentSummaryRow> rows) {
        try {
            Files.createDirectories(reportsRoot);
            Path reportFile = reportsRoot.resolve(reportFileName);
            try (BufferedWriter writer = Files.newBufferedWriter(
                    reportFile,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING
            )) {
                writer.write(SUMMARY_HEADER);
                writer.newLine();
                for (ExperimentSummaryRow row : rows) {
                    writer.write(toCsvRow(row));
                    writer.newLine();
                }
            }
            LOG.info("Report written to {}", reportFile.toAbsolutePath());
            return reportFile;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to write report", e);
        }
    }

    private String toCsvRow(ExperimentSummaryRow row) {
        return String.join(",",
                Objects.toString(row.topic(), ""),
                Objects.toString(row.runId(), ""),
                Integer.toString(row.messagesUnique()),
                Long.toString(row.totalPayloadBytes()),
                Double.toString(round3(row.durationSec())),
                Double.toString(round6(row.throughputMbps())),
                Long.toString(row.maxLatencyMs())
        );
    }

    private double round3(double value) {
        return Math.round(value * 1_000.0) / 1_000.0;
    }

    private double round6(double value) {
        return Math.round(value * 1_000_000.0) / 1_000_000.0;
    }
}
