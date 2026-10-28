package dev.joseherrera.inventory.availability;

import java.io.IOException;
import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import dev.joseherrera.inventory.events.StockLevelChanged;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;

@Component
class StockLevelDecoder {

    private final KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();

    StockLevelDecoder(@Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
        deserializer.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true), false);
    }

    StockLevelChanged decode(String topic, byte[] value) {
        StockLevelChanged event;
        try {
            event = (StockLevelChanged) deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            if (hasCause(e, IOException.class)) {
                throw e;
            }
            throw new InvalidEventException("undecodable stock level", e);
        } catch (ClassCastException e) {
            throw new InvalidEventException("not a StockLevelChanged", e);
        }
        if (event == null) {
            throw new InvalidEventException("empty stock level");
        }
        validate(event);
        return event;
    }

    private static void validate(StockLevelChanged event) {
        if (event.getSku().isBlank() || event.getLocation().isBlank()) {
            throw new InvalidEventException("stock level without sku or location");
        }
        if (event.getSourceLsn().isBlank()) {
            throw new InvalidEventException("stock level without source LSN");
        }
        if (event.getAvailable().compareTo(event.getOnHand().subtract(event.getReserved())) != 0) {
            throw new InvalidEventException("available is not onHand - reserved");
        }
    }

    static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        for (Throwable cause = error; cause != null; cause = cause.getCause()) {
            if (type.isInstance(cause)) {
                return true;
            }
        }
        return false;
    }
}
