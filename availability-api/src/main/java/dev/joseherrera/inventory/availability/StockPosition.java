package dev.joseherrera.inventory.availability;

import java.math.BigDecimal;
import java.time.Instant;

/** One SKU at one location, as last projected. {@code asOf} is the ERP time of the value. */
public record StockPosition(
        String location,
        BigDecimal onHand,
        BigDecimal reserved,
        BigDecimal available,
        Instant asOf) {
}
