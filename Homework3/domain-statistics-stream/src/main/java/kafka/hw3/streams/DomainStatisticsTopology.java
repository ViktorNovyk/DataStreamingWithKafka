package kafka.hw3.streams;

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

@Configuration
public class DomainStatisticsTopology {

    public static final String DOMAIN_COUNTS_STORE = "top-level-domain-counts-store";

    private static final Logger LOG = LoggerFactory.getLogger(DomainStatisticsTopology.class);

    @Bean
    KStream<String, BrowserHistoryVisitMessage> domainStatisticsStream(
            StreamsBuilder streamsBuilder,
            @Value("${app.input-topic:browser-history-visits}") String inputTopic
    ) {
        JacksonJsonSerde<BrowserHistoryVisitMessage> visitSerde = new JacksonJsonSerde<>(BrowserHistoryVisitMessage.class);
        visitSerde.ignoreTypeHeaders();

        KStream<String, BrowserHistoryVisitMessage> visits = streamsBuilder.stream(
                inputTopic,
                Consumed.with(Serdes.String(), visitSerde)
        );

        visits
                .filter((_, visit) -> visit != null && visit.visitCount() > 0)
                .selectKey((_, visit) -> DomainExtractor.extractTopLevelDomain(visit.url()).orElse(""))
                .filter((topLevelDomain, _) -> !topLevelDomain.isBlank())
                .groupByKey(Grouped.with(Serdes.String(), visitSerde))
                .aggregate(
                        () -> 0L,
                        (_, visit, totalVisits) -> totalVisits + visit.visitCount(),
                        Materialized.<String, Long, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(DOMAIN_COUNTS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Long())
                );

        LOG.info("Kafka Streams topology reads topic={} and materializes store={}", inputTopic, DOMAIN_COUNTS_STORE);
        return visits;
    }
}
