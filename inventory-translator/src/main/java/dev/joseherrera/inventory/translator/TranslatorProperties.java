package dev.joseherrera.inventory.translator;

import java.time.ZoneId;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("translator")
public record TranslatorProperties(
        String movementsInputTopic,
        String stockInputTopic,
        String movementsOutputTopic,
        String stockLevelsOutputTopic,
        String deadLetterTopic,
        ZoneId legacyTimezone) {
}
