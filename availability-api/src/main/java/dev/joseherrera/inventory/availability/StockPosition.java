package dev.joseherrera.inventory.availability;

import java.math.BigDecimal;
import java.time.Instant;

public record StockPosition(
        String location,
        BigDecimal onHand,
        BigDecimal reserved,
        BigDecimal available,
        Instant asOf) {
}
