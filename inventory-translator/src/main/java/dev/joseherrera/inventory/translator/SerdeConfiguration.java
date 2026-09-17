package dev.joseherrera.inventory.translator;

import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.JsonNode;

import dev.joseherrera.inventory.events.StockLevelChanged;
import dev.joseherrera.inventory.events.StockMoved;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@Configuration
class SerdeConfiguration {

    private final String schemaRegistryUrl;

    SerdeConfiguration(@Value("${spring.kafka.properties.schema.registry.url}") String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    @Bean
    Serde<JsonNode> changeEventSerde() {
        return new ChangeEventSerde();
    }

    @Bean
    Serde<StockMoved> stockMovedSerde() {
        return avroSerde(schemaRegistryUrl);
    }

    @Bean
    Serde<StockLevelChanged> stockLevelSerde() {
        return avroSerde(schemaRegistryUrl);
    }

    static <T extends SpecificRecord> Serde<T> avroSerde(String schemaRegistryUrl) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(Map.of(
                AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl,
                // Contracts are published deliberately, not by whatever the app happens to send.
                AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, "false",
                AbstractKafkaSchemaSerDeConfig.USE_LATEST_VERSION, "true"), false);
        return serde;
    }
}
