package dev.joseherrera.inventory.availability;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class AvailabilityApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(AvailabilityApiApplication.class, args);
    }
}
