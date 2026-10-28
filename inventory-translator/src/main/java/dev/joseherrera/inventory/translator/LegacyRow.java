package dev.joseherrera.inventory.translator;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;

final class LegacyRow {

    private LegacyRow() {
    }

    static String text(JsonNode row, String column) {
        JsonNode value = row.get(column);
        return value == null || value.isNull() ? null : value.asText().strip();
    }

    static BigDecimal decimal(JsonNode row, String column) {
        String value = text(row, column);
        return value == null ? BigDecimal.ZERO : new BigDecimal(value);
    }

    static long number(JsonNode row, String column) {
        JsonNode value = row.get(column);
        return value == null || value.isNull() ? 0L : value.asLong();
    }

    static Instant timestamp(JsonNode row, String column, ZoneId legacyZone) {
        long millis = number(row, column);
        LocalDateTime wallClock = LocalDateTime.ofEpochSecond(
                TimeUnit.MILLISECONDS.toSeconds(millis),
                (int) TimeUnit.MILLISECONDS.toNanos(Math.floorMod(millis, 1_000L)),
                ZoneOffset.UTC);
        return wallClock.atZone(legacyZone).toInstant();
    }
}
