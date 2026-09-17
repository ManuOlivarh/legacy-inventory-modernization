package dev.joseherrera.inventory.translator;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Reads the legacy shapes out of a Debezium change event. Every quirk of the ERP
 * is handled here and nowhere else: that is what makes this an anti-corruption layer.
 */
final class LegacyRow {

    private LegacyRow() {
    }

    /** CHAR columns arrive padded to their fixed width. */
    static String text(JsonNode row, String column) {
        JsonNode value = row.get(column);
        return value == null || value.isNull() ? null : value.asText().strip();
    }

    /** decimal.handling.mode=string keeps the exact value the ERP holds. */
    static BigDecimal decimal(JsonNode row, String column) {
        String value = text(row, column);
        return value == null ? BigDecimal.ZERO : new BigDecimal(value);
    }

    static long number(JsonNode row, String column) {
        JsonNode value = row.get(column);
        return value == null || value.isNull() ? 0L : value.asLong();
    }

    /**
     * Debezium maps SQL Server DATETIME to milliseconds since epoch, reading the value
     * as if it were UTC. The ERP server actually runs in local time, so the wall-clock
     * value is correct but the zone is not: it has to be reinterpreted, not shifted.
     */
    static Instant timestamp(JsonNode row, String column, ZoneId legacyZone) {
        long millis = number(row, column);
        LocalDateTime wallClock = LocalDateTime.ofEpochSecond(
                TimeUnit.MILLISECONDS.toSeconds(millis),
                (int) TimeUnit.MILLISECONDS.toNanos(Math.floorMod(millis, 1_000L)),
                ZoneOffset.UTC);
        return wallClock.atZone(legacyZone).toInstant();
    }
}
