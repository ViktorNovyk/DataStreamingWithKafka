package kafka.hw3.streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
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
public class TopFiveDomainLogger {

    private static final Logger LOG = LoggerFactory.getLogger(TopFiveDomainLogger.class);
    public static final StoreQueryParameters<ReadOnlyKeyValueStore<String, Long>> storeQueryParams = StoreQueryParameters.fromNameAndType(
            DomainStatisticsTopology.DOMAIN_COUNTS_STORE,
            QueryableStoreTypes.keyValueStore()
    );

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public TopFiveDomainLogger(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    @Scheduled(fixedDelayString = "${app.top-five-log-interval-ms:10000}")
    public void logTopFiveDomains() {
        KafkaStreams kafkaStreams = streamsBuilderFactoryBean.getKafkaStreams();
        if (kafkaStreams == null) {
            LOG.info("Kafka Streams is not started yet");
            return;
        }
        KafkaStreams.State state = kafkaStreams.state();
        if (!state.isRunningOrRebalancing()) {
            LOG.warn("Skipping top-level domain state store query because Kafka Streams state is {}", state);
            return;
        }

        final ReadOnlyKeyValueStore<String, Long> store = kafkaStreams.store(storeQueryParams);
        try (Stream<DomainCount> counts = domainCounts(store)) {
            LOG.info("");
            LOG.info("Top 5 domains with most visits:");
            counts.sorted(Comparator.comparingLong(DomainCount::visits).reversed())
                    .limit(5)
                    .forEach(dc -> LOG.info("{}: {}", dc.domain(), dc.visits()));
        }
    }

    private Stream<DomainCount> domainCounts(ReadOnlyKeyValueStore<String, Long> store) {
        final var iterator = store.all();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                .onClose(iterator::close)
                .map(entry -> new DomainCount(entry.key, entry.value));
    }

    private record DomainCount(String domain, long visits) {
    }
}
