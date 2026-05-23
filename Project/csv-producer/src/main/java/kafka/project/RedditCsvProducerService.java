package kafka.project;

import com.fasterxml.uuid.Generators;
import com.fasterxml.uuid.NoArgGenerator;
import com.univocity.parsers.common.TextParsingException;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.UnescapedQuoteHandling;
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

@Service
public class RedditCsvProducerService {

    private static final Logger LOG = LoggerFactory.getLogger(RedditCsvProducerService.class);
    private static final int TEXT_COLUMN = 1;
    private static final int DEFAULT_MAX_CHARS_PER_COLUMN = 262144;
    private static final int DEFAULT_INPUT_BUFFER_SIZE = DEFAULT_MAX_CHARS_PER_COLUMN;

    private final KafkaTemplate<String, RawCommentMessage> kafkaTemplate;
    private final String rawTopic;
    private final Clock clock;
    private final int csvMaxCharsPerColumn;
    private final int csvInputBufferSize;
    private final NoArgGenerator commentIdGenerator;

    public RedditCsvProducerService(
            KafkaTemplate<String, RawCommentMessage> kafkaTemplate,
            @Value("${app.raw-topic:reddit-raw-comments}") String rawTopic,
            @Value("${app.csv.max-chars-per-column:" + DEFAULT_MAX_CHARS_PER_COLUMN + "}") int csvMaxCharsPerColumn,
            @Value("${app.csv.input-buffer-size:" + DEFAULT_INPUT_BUFFER_SIZE + "}") int csvInputBufferSize
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.rawTopic = rawTopic;
        this.csvMaxCharsPerColumn = csvMaxCharsPerColumn;
        this.csvInputBufferSize = csvInputBufferSize;
        this.clock = Clock.systemUTC();
        this.commentIdGenerator = Generators.timeBasedEpochGenerator();
    }

    public UploadRedditCsvResponse produce(MultipartFile file) {
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

        LOG.info("Produced {} reddit comments to topic={}", producedMessages, rawTopic);
        return new UploadRedditCsvResponse(rawTopic, producedMessages, "COMPLETED");
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
                RawCommentMessage message = toMessage(row, rowNumber);
                kafkaTemplate.send(rawTopic, message.commentId(), message);
                producedMessages++;
            }
        } catch (TextParsingException | ArrayIndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Invalid CSV near row " + (rowNumber + 1)
                    + ". Expected columns: language,text,correction.", e);
        } finally {
            parser.stopParsing();
        }

        return producedMessages;
    }

    private RawCommentMessage toMessage(String[] row, long rowNumber) {
        if (row.length < 2) {
            throw new IllegalArgumentException("CSV row " + rowNumber + " must contain at least language,text columns");
        }

        String text = row[TEXT_COLUMN] == null ? "" : row[TEXT_COLUMN].trim();
        if (text.isBlank()) {
            throw new IllegalArgumentException("CSV row " + rowNumber + " has empty text");
        }

        long now = clock.millis();
        return new RawCommentMessage(commentIdGenerator.generate().toString(), text, now);
    }

    private boolean isHeader(String[] row) {
        return row.length >= 3
                && "language".equalsIgnoreCase(row[0].trim())
                && "text".equalsIgnoreCase(row[1].trim())
                && "correction".equalsIgnoreCase(row[2].trim());
    }

    private CsvParserSettings parserSettings() {
        validateCsvParserLimits();
        CsvParserSettings settings = new CsvParserSettings();
        settings.setHeaderExtractionEnabled(false);
        settings.setMaxColumns(3);
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
