package dev.joseherrera.inventory.translator;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Reads Debezium change events as plain JSON. The raw topics have no schema by
 * design: they are internal and only this service reads them.
 */
final class ChangeEventSerde implements Serde<JsonNode> {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public Serializer<JsonNode> serializer() {
        return (topic, data) -> {
            try {
                return data == null ? null : mapper.writeValueAsBytes(data);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    @Override
    public Deserializer<JsonNode> deserializer() {
        return (topic, data) -> {
            // Tombstones are legitimate: Kafka writes them when a row is deleted.
            if (data == null || data.length == 0) {
                return null;
            }
            try {
                return mapper.readTree(data);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }
}
