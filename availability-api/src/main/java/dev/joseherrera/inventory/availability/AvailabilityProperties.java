package dev.joseherrera.inventory.availability;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param stockLevelsTopic the public contract this read model projects
 * @param deadLetterTopic  where records that cannot be applied go, byte for byte
 */
@ConfigurationProperties("availability")
public record AvailabilityProperties(String stockLevelsTopic, String deadLetterTopic) {
}
