package kafka.hw2.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class KafkaMessageConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageConsumer.class);

    private final ConsumerLogWriter logWriter;
    private final String consumerId;

    public KafkaMessageConsumer(
            ConsumerLogWriter logWriter,
            @Value("${CONSUMER_ID:}") String configuredConsumerId,
            @Value("${HOSTNAME:}") String hostname
    ) {
        this.logWriter = logWriter;
        if (StringUtils.hasText(configuredConsumerId)) {
            this.consumerId = configuredConsumerId;
        } else if (StringUtils.hasText(hostname)) {
            this.consumerId = hostname;
        } else {
            this.consumerId = "consumer-" + UUID.randomUUID();
        }
    }

    @KafkaListener(
            topicPattern = "${app.consumer.topic-pattern:hw2-exp-.*}",
            groupId = "${app.consumer.group-id:hw2-consumers}"
    )
    public void consume(
            ExperimentMessage message,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        sleepOneSecond();
        long processedAtMs = System.currentTimeMillis();
        ConsumerLogRow row = new ConsumerLogRow(
                message.messageId(),
                topic,
                message.runId(),
                partition,
                offset,
                consumerId,
                message.sentAtMs(),
                processedAtMs,
                message.payloadSizeBytes()
        );
        logWriter.write(row);
        LOG.info(
                "Processed message_id={} topic={} run_id={} partition={} offset={} consumer_id={}",
                message.messageId(),
                topic,
                message.runId(),
                partition,
                offset,
                consumerId
        );
    }

    private void sleepOneSecond() {
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Consumer interrupted during processing delay", e);
        }
    }
}
