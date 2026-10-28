package dev.joseherrera.inventory.availability;

import java.time.Clock;
import java.time.Duration;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import dev.joseherrera.inventory.events.StockLevelChanged;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

@Component
class StockLevelListener {

    private final StockLevelDecoder decoder;
    private final StockLevelRepository repository;
    private final Clock clock;
    private final Timer freshness;

    StockLevelListener(StockLevelDecoder decoder, StockLevelRepository repository, Clock clock, MeterRegistry meters) {
        this.decoder = decoder;
        this.repository = repository;
        this.clock = clock;
        this.freshness = Timer.builder("inventory.freshness")
                .description("Time from the ERP change to the value being served by the API")
                .serviceLevelObjectives(Duration.ofSeconds(1), Duration.ofSeconds(2), Duration.ofSeconds(5),
                        Duration.ofSeconds(10), Duration.ofSeconds(30))
                .register(meters);
    }

    @KafkaListener(topics = "${availability.stock-levels-topic}")
    void on(ConsumerRecord<String, byte[]> record) {
        StockLevelChanged event = decoder.decode(record.topic(), record.value());
        if (repository.apply(event)) {
            freshness.record(Duration.between(event.getChangedAt(), clock.instant()));
        }
    }
}
