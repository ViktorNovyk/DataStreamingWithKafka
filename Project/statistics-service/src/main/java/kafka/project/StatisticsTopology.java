package kafka.project;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;

import java.util.Locale;

@Configuration
public class StatisticsTopology {

    public static final String LANGUAGE_COUNTS_STORE = "project-language-counts-store";
    public static final String SENTIMENT_COUNTS_STORE = "project-sentiment-counts-store";
    public static final String KEYWORD_COUNTS_STORE = "project-keyword-counts-store";

    private static final Logger LOG = LoggerFactory.getLogger(StatisticsTopology.class);

    @Bean
    KStream<String, LanguageDetectedCommentMessage> languageStatisticsStream(
            StreamsBuilder streamsBuilder,
            @Value("${app.language-topic:reddit-comments-with-language}") String languageTopic
    ) {
        JacksonJsonSerde<LanguageDetectedCommentMessage> serde = new JacksonJsonSerde<>(LanguageDetectedCommentMessage.class);
        serde.ignoreTypeHeaders();

        KStream<String, LanguageDetectedCommentMessage> stream = streamsBuilder.stream(languageTopic, Consumed.with(Serdes.String(), serde));
        stream
                .filter((_, value) -> value != null && value.language() != null && !value.language().isBlank())
                .selectKey((_, value) -> value.language().toLowerCase(Locale.ROOT))
                .groupByKey(Grouped.with(Serdes.String(), serde))
                .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(LANGUAGE_COUNTS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        LOG.info("Language statistics consume topic={} store={}", languageTopic, LANGUAGE_COUNTS_STORE);
        return stream;
    }

    @Bean
    KStream<String, SentimentResultMessage> sentimentStatisticsStream(
            StreamsBuilder streamsBuilder,
            @Value("${app.sentiment-topic:reddit-comments-sentiment}") String sentimentTopic
    ) {
        JacksonJsonSerde<SentimentResultMessage> serde = new JacksonJsonSerde<>(SentimentResultMessage.class);
        serde.ignoreTypeHeaders();

        KStream<String, SentimentResultMessage> stream = streamsBuilder.stream(sentimentTopic, Consumed.with(Serdes.String(), serde));
        stream
                .filter((_, value) -> value != null && value.sentiment() != null && !value.sentiment().isBlank())
                .selectKey((_, value) -> value.sentiment())
                .groupByKey(Grouped.with(Serdes.String(), serde))
                .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(SENTIMENT_COUNTS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        LOG.info("Sentiment statistics consume topic={} store={}", sentimentTopic, SENTIMENT_COUNTS_STORE);
        return stream;
    }

    @Bean
    KStream<String, KeywordResultMessage> keywordStatisticsStream(
            StreamsBuilder streamsBuilder,
            @Value("${app.keyword-topic:reddit-comments-keywords}") String keywordTopic
    ) {
        JacksonJsonSerde<KeywordResultMessage> serde = new JacksonJsonSerde<>(KeywordResultMessage.class);
        serde.ignoreTypeHeaders();

        KStream<String, KeywordResultMessage> stream = streamsBuilder.stream(keywordTopic, Consumed.with(Serdes.String(), serde));
        stream
                .filter((_, value) -> value != null && value.keywords() != null)
                .flatMapValues(KeywordResultMessage::keywords)
                .filter((_, keyword) -> keyword != null && !keyword.isBlank())
                .selectKey((_, keyword) -> keyword.toLowerCase(Locale.ROOT))
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                .count(Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(KEYWORD_COUNTS_STORE)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()));

        LOG.info("Keyword statistics consume topic={} store={}", keywordTopic, KEYWORD_COUNTS_STORE);
        return stream;
    }
}
