package dev.joseherrera.inventory.simulator;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class ErpSimulatorApplication {

    public static void main(String[] args) {
        SpringApplication.run(ErpSimulatorApplication.class, args);
    }
}
