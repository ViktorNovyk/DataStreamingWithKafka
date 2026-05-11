package kafka.hw2.producer;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.UnescapedQuoteHandling;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
public class ExperimentProducerService {

    private static final Logger LOG = LoggerFactory.getLogger(ExperimentProducerService.class);
    private static final DateTimeFormatter RUN_ID_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss'Z'")
            .withZone(ZoneOffset.UTC);
    private static final String[] CSV_HEADERS = {
            "text",
            "id",
            "subreddit",
            "meta",
            "time",
            "author",
            "ups",
            "downs",
            "authorlinkkarma",
            "authorkarma",
            "authorisgold"
    };
    private static final int DEFAULT_MAX_CHARS_PER_COLUMN = 128 * 1024;
    private static final int DEFAULT_MAX_COLUMNS = 64;
    private static final int DEFAULT_INPUT_BUFFER_SIZE = 64 * 1024;

    private final KafkaTemplate<Object, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final Clock clock;
    private final String topicPrefix;
    private final String producerId;
    private final int csvMaxCharsPerColumn;
    private final int csvMaxColumns;
    private final int csvInputBufferSize;

    public ExperimentProducerService(
            KafkaTemplate<Object, Object> kafkaTemplate,
            ObjectMapper objectMapper,
            @Value("${app.topic-prefix:hw2-exp-}") String topicPrefix,
            @Value("${app.producer-id:csv-producer}") String producerId,
            @Value("${app.csv.max-chars-per-column:" + DEFAULT_MAX_CHARS_PER_COLUMN + "}") int csvMaxCharsPerColumn,
            @Value("${app.csv.max-columns:" + DEFAULT_MAX_COLUMNS + "}") int csvMaxColumns,
            @Value("${app.csv.input-buffer-size:" + DEFAULT_INPUT_BUFFER_SIZE + "}") int csvInputBufferSize
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topicPrefix = topicPrefix;
        this.producerId = producerId;
        this.csvMaxCharsPerColumn = csvMaxCharsPerColumn;
        this.csvMaxColumns = csvMaxColumns;
        this.csvInputBufferSize = csvInputBufferSize;
        this.clock = Clock.systemUTC();
        this.httpClient = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build();
    }

    public StartExperimentResponse start(StartExperimentRequest request) {
        int shardCount = request.shardCount() == null ? 1 : request.shardCount();
        int shardIndex = request.shardIndex() == null ? 0 : request.shardIndex();
        validateShardConfig(shardIndex, shardCount);

        String runId = StringUtils.hasText(request.runId()) ? request.runId() : RUN_ID_FORMATTER.format(Instant.now(clock));
        String topicName = topicPrefix + request.experimentName().toLowerCase();
        long startedAtMs = clock.millis();
        long producedMessages = streamCsvAndProduce(request.csvUrl(), request.experimentName(), topicName, runId, shardIndex, shardCount);
        kafkaTemplate.flush();
        long finishedAtMs = clock.millis();

        return new StartExperimentResponse(
                request.experimentName(),
                topicName,
                runId,
                shardIndex,
                shardCount,
                producedMessages,
                startedAtMs,
                finishedAtMs,
                "COMPLETED"
        );
    }

    private long streamCsvAndProduce(
            String csvUrl,
            String experimentName,
            String topicName,
            String runId,
            int shardIndex,
            int shardCount
    ) {
        HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(csvUrl))
                .GET()
                .build();

        try {
            HttpResponse<InputStream> response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream());
            if (response.statusCode() >= 400) {
                throw new IllegalStateException("Failed to read CSV. HTTP status: " + response.statusCode());
            }
            return produceCsvStream(response.body(), experimentName, topicName, runId, shardIndex, shardCount);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("CSV streaming interrupted", e);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read CSV stream", e);
        }
    }

    private long produceCsvStream(
            InputStream csvStream,
            String experimentName,
            String topicName,
            String runId,
            int shardIndex,
            int shardCount
    ) throws IOException {
        CsvParserSettings settings = createCsvParserSettings();

        long producedMessages = 0;
        long rowIndex = 0;

        try (InputStream buffered = new BufferedInputStream(csvStream, 256 * 1024);
             Reader reader = new InputStreamReader(buffered, StandardCharsets.UTF_8)) {

            CsvParser parser = new CsvParser(settings);
            parser.beginParsing(reader);
            String[] row;
            while ((row = parser.parseNext()) != null) {
                if (rowIndex % shardCount == shardIndex) {
                    String payload = toPayload(row);
                    int payloadSizeBytes = payload.getBytes(StandardCharsets.UTF_8).length;
                    ExperimentMessage message = new ExperimentMessage(
                            UUID.randomUUID().toString(),
                            runId,
                            producerId,
                            shardIndex,
                            shardCount,
                            experimentName,
                            clock.millis(),
                            payloadSizeBytes,
                            payload
                    );
                    kafkaTemplate.send(topicName, message.messageId(), message);
                    producedMessages++;
                }
                rowIndex++;
            }
            parser.stopParsing();
        }

        LOG.info(
                "Produced {} messages for experiment={} topic={} runId={} shard={}/{}",
                producedMessages,
                experimentName,
                topicName,
                runId,
                shardIndex,
                shardCount
        );
        return producedMessages;
    }

    private CsvParserSettings createCsvParserSettings() {
        validateCsvParserLimits();
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(false);
        settings.setHeaders(CSV_HEADERS);
        settings.setMaxCharsPerColumn(csvMaxCharsPerColumn);
        settings.setMaxColumns(csvMaxColumns);
        settings.setInputBufferSize(csvInputBufferSize);
        settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.BACK_TO_DELIMITER);
        settings.setLineSeparatorDetectionEnabled(true);
        return settings;
    }

    private String toPayload(String[] row) {
        Map<String, String> csvRecord = new LinkedHashMap<>(CSV_HEADERS.length);
        for (int i = 0; i < CSV_HEADERS.length; i++) {
            String value = i < row.length ? row[i] : "";
            csvRecord.put(CSV_HEADERS[i], value);
        }
        try {
            return objectMapper.writeValueAsString(csvRecord);
        } catch (JacksonException e) {
            throw new IllegalStateException("Failed to serialize CSV row", e);
        }
    }

    private void validateShardConfig(int shardIndex, int shardCount) {
        if (shardCount <= 0) {
            throw new IllegalArgumentException("shardCount must be greater than 0");
        }
        if (shardIndex < 0 || shardIndex >= shardCount) {
            throw new IllegalArgumentException("shardIndex must be in range [0, shardCount)");
        }
    }

    private void validateCsvParserLimits() {
        if (csvMaxCharsPerColumn < 4096) {
            throw new IllegalArgumentException("app.csv.max-chars-per-column must be at least 4096");
        }
        if (csvMaxColumns <= 0) {
            throw new IllegalArgumentException("app.csv.max-columns must be greater than 0");
        }
        if (csvInputBufferSize <= 0) {
            throw new IllegalArgumentException("app.csv.input-buffer-size must be greater than 0");
        }
    }
}
