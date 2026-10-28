package dev.joseherrera.inventory.availability;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("availability")
public record AvailabilityProperties(String stockLevelsTopic, String deadLetterTopic) {
}
