package dev.joseherrera.inventory.translator;

import java.time.ZoneId;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * @param deadLetterTopic where raw change events that cannot be translated go, unchanged
 * @param legacyTimezone the ERP stores DATETIME without a zone; this is the zone those
 *                       timestamps are written in, used to convert them to UTC instants
 */
@ConfigurationProperties("translator")
public record TranslatorProperties(
        String movementsInputTopic,
        String stockInputTopic,
        String movementsOutputTopic,
        String stockLevelsOutputTopic,
        String deadLetterTopic,
        ZoneId legacyTimezone) {
}
