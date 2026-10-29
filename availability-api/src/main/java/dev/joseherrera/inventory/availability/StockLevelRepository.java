package dev.joseherrera.inventory.availability;

import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

import dev.joseherrera.inventory.events.StockLevelChanged;

@Repository
class StockLevelRepository {

    private static final String UPSERT = """
            INSERT INTO stock_level (sku, location, on_hand, reserved, available, changed_at, source_lsn, updated_at)
            VALUES (:sku, :location, :onHand, :reserved, :available, :changedAt, :sourceLsn, :updatedAt)
            ON CONFLICT (sku, location) DO UPDATE SET
                on_hand = excluded.on_hand,
                reserved = excluded.reserved,
                available = excluded.available,
                changed_at = excluded.changed_at,
                source_lsn = excluded.source_lsn,
                updated_at = excluded.updated_at
            WHERE excluded.source_lsn > stock_level.source_lsn
               OR (excluded.source_lsn = stock_level.source_lsn
                   AND (excluded.on_hand, excluded.reserved, excluded.changed_at)
                       IS DISTINCT FROM (stock_level.on_hand, stock_level.reserved, stock_level.changed_at))
            """;

    private final JdbcClient jdbc;
    private final Clock clock;

    StockLevelRepository(JdbcClient jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    boolean apply(StockLevelChanged event) {
        return jdbc.sql(UPSERT)
                .param("sku", event.getSku())
                .param("location", event.getLocation())
                .param("onHand", event.getOnHand())
                .param("reserved", event.getReserved())
                .param("available", event.getAvailable())
                .param("changedAt", Timestamp.from(event.getChangedAt()))
                .param("sourceLsn", event.getSourceLsn())
                .param("updatedAt", Timestamp.from(clock.instant()))
                .update() == 1;
    }

    List<StockPosition> findBySku(String sku) {
        return jdbc.sql("""
                        SELECT location, on_hand, reserved, available, changed_at
                        FROM stock_level WHERE sku = :sku ORDER BY location
                        """)
                .param("sku", sku)
                .query((rs, row) -> new StockPosition(
                        rs.getString("location"),
                        rs.getBigDecimal("on_hand"),
                        rs.getBigDecimal("reserved"),
                        rs.getBigDecimal("available"),
                        rs.getTimestamp("changed_at").toInstant()))
                .list();
    }
}
