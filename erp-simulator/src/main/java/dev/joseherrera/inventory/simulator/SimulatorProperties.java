package dev.joseherrera.inventory.simulator;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("simulator")
public record SimulatorProperties(double movementsPerSecond) {

    public SimulatorProperties {
        if (movementsPerSecond <= 0) {
            throw new IllegalArgumentException("simulator.movements-per-second must be positive");
        }
    }
}
