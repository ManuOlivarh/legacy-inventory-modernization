package dev.joseherrera.inventory.translator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableKafkaStreams
public class InventoryTranslatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryTranslatorApplication.class, args);
    }
}
