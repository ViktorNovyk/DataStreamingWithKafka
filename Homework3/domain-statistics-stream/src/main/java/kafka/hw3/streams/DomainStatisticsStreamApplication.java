package kafka.hw3.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@EnableKafkaStreams
@SpringBootApplication
public class DomainStatisticsStreamApplication {

    static void main(String[] args) {
        SpringApplication.run(DomainStatisticsStreamApplication.class, args);
    }
}
