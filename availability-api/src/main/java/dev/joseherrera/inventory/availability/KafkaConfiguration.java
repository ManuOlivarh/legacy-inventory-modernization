package dev.joseherrera.inventory.availability;

import java.io.IOException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.time.Clock;
import java.time.Duration;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@Configuration
class KafkaConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> template, AvailabilityProperties properties) {
        var recoverer = new DeadLetterPublishingRecoverer(template,
                (record, error) -> new TopicPartition(properties.deadLetterTopic(), -1));

        ExponentialBackOff untilRecovered = new ExponentialBackOff(500, 2.0);
        untilRecovered.setMaxInterval(Duration.ofSeconds(30).toMillis());

        ExponentialBackOff fewAttempts = new ExponentialBackOff(500, 2.0);
        fewAttempts.setMaxAttempts(3);

        var handler = new DefaultErrorHandler(recoverer, fewAttempts);
        handler.setBackOffFunction((record, error) -> isOutage(error) ? untilRecovered : fewAttempts);
        handler.addNotRetryableExceptions(InvalidEventException.class);
        return handler;
    }

    static boolean isOutage(Exception error) {
        return StockLevelDecoder.hasCause(error, TransientDataAccessException.class)
                || StockLevelDecoder.hasCause(error, DataAccessResourceFailureException.class)
                || StockLevelDecoder.hasCause(error, RecoverableDataAccessException.class)
                || StockLevelDecoder.hasCause(error, SQLTransientException.class)
                || StockLevelDecoder.hasCause(error, SQLRecoverableException.class)
                || (StockLevelDecoder.hasCause(error, SerializationException.class)
                    && StockLevelDecoder.hasCause(error, IOException.class));
    }
}
