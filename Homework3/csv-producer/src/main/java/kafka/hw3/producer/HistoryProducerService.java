package kafka.hw3.producer;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.UnescapedQuoteHandling;
import com.univocity.parsers.common.TextParsingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.UUID;

@Service
public class HistoryProducerService {

    private static final Logger LOG = LoggerFactory.getLogger(HistoryProducerService.class);
    private static final int URL_COLUMN = 0;
    private static final int VISIT_COUNT_COLUMN = 1;
    private static final int DEFAULT_MAX_CHARS_PER_COLUMN = 131072;
    private static final int DEFAULT_INPUT_BUFFER_SIZE = DEFAULT_MAX_CHARS_PER_COLUMN;

    private final KafkaTemplate<String, BrowserHistoryVisitMessage> kafkaTemplate;
    private final Clock clock;
    private final String inputTopic;
    private final int csvMaxCharsPerColumn;
    private final int csvInputBufferSize;

    public HistoryProducerService(
            KafkaTemplate<String, BrowserHistoryVisitMessage> kafkaTemplate,
            @Value("${app.input-topic:browser-history-visits}") String inputTopic,
            @Value("${app.csv.max-chars-per-column:" + DEFAULT_MAX_CHARS_PER_COLUMN + "}") int csvMaxCharsPerColumn,
            @Value("${app.csv.input-buffer-size:" + DEFAULT_INPUT_BUFFER_SIZE + "}") int csvInputBufferSize
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.inputTopic = inputTopic;
        this.csvMaxCharsPerColumn = csvMaxCharsPerColumn;
        this.csvInputBufferSize = csvInputBufferSize;
        this.clock = Clock.systemUTC();
    }

    public UploadHistoryResponse produce(MultipartFile file) {
        if (file == null || file.isEmpty()) {
            throw new IllegalArgumentException("CSV file must not be empty");
        }

        long producedMessages;
        try (InputStream inputStream = new BufferedInputStream(file.getInputStream(), 64 * 1024);
             Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8)) {
            producedMessages = parseAndProduce(reader);
            kafkaTemplate.flush();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read uploaded CSV file", e);
        }

        LOG.info("Produced {} browser history messages to topic={}", producedMessages, inputTopic);
        return new UploadHistoryResponse(inputTopic, producedMessages, "COMPLETED");
    }

    private long parseAndProduce(Reader reader) {
        CsvParser parser = new CsvParser(parserSettings());
        long producedMessages = 0;
        long rowNumber = 0;

        try {
            parser.beginParsing(reader);
            String[] row;
            while ((row = parser.parseNext()) != null) {
                rowNumber++;
                if (isHeader(row)) {
                    continue;
                }
                BrowserHistoryVisitMessage message = toMessage(row, rowNumber);
                kafkaTemplate.send(inputTopic, message.url(), message);
                producedMessages++;
            }
        } catch (TextParsingException | ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid CSV near row " + (rowNumber + 1)
                    + ". Ensure the file has url and visit_count columns and no field exceeds "
                    + csvMaxCharsPerColumn + " characters.", e);
        } finally {
            parser.stopParsing();
        }

        return producedMessages;
    }

    private BrowserHistoryVisitMessage toMessage(String[] row, long rowNumber) {
        if (row.length < 2) {
            throw new IllegalArgumentException("CSV row " + rowNumber + " must contain url and visit_count columns");
        }

        String url = row[URL_COLUMN] == null ? "" : row[URL_COLUMN].trim();
        if (url.isBlank()) {
            throw new IllegalArgumentException("CSV row " + rowNumber + " has empty url");
        }

        long visitCount;
        try {
            visitCount = Long.parseLong(row[VISIT_COUNT_COLUMN].trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("CSV row " + rowNumber + " has invalid visit_count", e);
        }
        if (visitCount < 0) {
            throw new IllegalArgumentException("CSV row " + rowNumber + " has negative visit_count");
        }

        return new BrowserHistoryVisitMessage(UUID.randomUUID().toString(), url, visitCount, clock.millis());
    }

    private boolean isHeader(String[] row) {
        return row.length >= 2
                && "url".equalsIgnoreCase(row[URL_COLUMN].trim())
                && "visit_count".equalsIgnoreCase(row[VISIT_COUNT_COLUMN].trim());
    }

    private CsvParserSettings parserSettings() {
        validateCsvParserLimits();
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(false);
        settings.setMaxColumns(2);
        settings.setMaxCharsPerColumn(csvMaxCharsPerColumn);
        settings.setInputBufferSize(csvInputBufferSize);
        settings.setUnescapedQuoteHandling(UnescapedQuoteHandling.BACK_TO_DELIMITER);
        settings.setLineSeparatorDetectionEnabled(true);
        return settings;
    }

    private void validateCsvParserLimits() {
        if (csvMaxCharsPerColumn < 4096) {
            throw new IllegalArgumentException("app.csv.max-chars-per-column must be at least 4096");
        }
        if (csvInputBufferSize < csvMaxCharsPerColumn) {
            throw new IllegalArgumentException("app.csv.input-buffer-size must be at least app.csv.max-chars-per-column");
        }
    }
}
