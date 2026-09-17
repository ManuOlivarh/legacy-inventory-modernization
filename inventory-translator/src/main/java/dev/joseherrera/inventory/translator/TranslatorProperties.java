package dev.joseherrera.inventory.translator;

import java.time.ZoneId;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param legacyTimezone the ERP stores DATETIME without a zone; this is the zone those
 *                       timestamps are written in, used to convert them to UTC instants
 */
@ConfigurationProperties("translator")
public record TranslatorProperties(
        String movementsInputTopic,
        String stockInputTopic,
        String movementsOutputTopic,
        String stockLevelsOutputTopic,
        ZoneId legacyTimezone) {
}
