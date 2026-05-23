package kafka.project;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Component
public class StatisticsLogger {

    private static final Logger LOG = LoggerFactory.getLogger(StatisticsLogger.class);

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public StatisticsLogger(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @Scheduled(fixedDelayString = "${app.statistics-log-interval-ms:10000}")
    public void logStatistics() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null || !kafkaStreams.state().isRunningOrRebalancing()) {
            LOG.info("Kafka Streams is not ready for statistics queries");
            return;
        }

        LOG.info("");
        LOG.info("Languages:");
        logAll(kafkaStreams, StatisticsTopology.LANGUAGE_COUNTS_STORE, Long.MAX_VALUE);
        LOG.info("Sentiments:");
        logAll(kafkaStreams, StatisticsTopology.SENTIMENT_COUNTS_STORE, Long.MAX_VALUE);
        LOG.info("Top 10 keywords:");
        logAll(kafkaStreams, StatisticsTopology.KEYWORD_COUNTS_STORE, 10);
    }

    private void logAll(KafkaStreams kafkaStreams, String storeName, long limit) {
        ReadOnlyKeyValueStore<String, Long> store;
        try {
            store = kafkaStreams.store(StoreQueryParameters.fromNameAndType(
                    storeName,
                    QueryableStoreTypes.keyValueStore()
            ));
        } catch (InvalidStateStoreException e) {
            LOG.info("Statistics store={} is not queryable yet; Kafka Streams state={}", storeName, kafkaStreams.state());
            return;
        }

        try (Stream<CountRow> rows = rows(store)) {
            rows.sorted(Comparator.comparingLong(CountRow::count).reversed().thenComparing(CountRow::name))
                    .limit(limit)
                    .forEach(row -> LOG.info("{}: {}", row.name(), row.count()));
        }
    }

    private Stream<CountRow> rows(ReadOnlyKeyValueStore<String, Long> store) {
        final var iterator = store.all();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                .onClose(iterator::close)
                .map(entry -> new CountRow(entry.key, entry.value));
    }

    private record CountRow(String name, long count) {
    }
}
