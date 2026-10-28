package dev.joseherrera.inventory.availability;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;

@RestController
class AvailabilityController {

    private final StockLevelRepository repository;

    AvailabilityController(StockLevelRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/availability/{sku}")
    ResponseEntity<Availability> availability(@PathVariable String sku) {
        List<StockPosition> locations = repository.findBySku(sku);
        if (locations.isEmpty()) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(new Availability(
                sku,
                sum(locations, StockPosition::onHand),
                sum(locations, StockPosition::reserved),
                sum(locations, StockPosition::available),
                locations.stream().map(StockPosition::asOf).min(Comparator.naturalOrder()).orElseThrow(),
                locations));
    }

    private static BigDecimal sum(List<StockPosition> locations, java.util.function.Function<StockPosition, BigDecimal> field) {
        return locations.stream().map(field).reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    record Availability(
            String sku,
            BigDecimal onHand,
            BigDecimal reserved,
            BigDecimal available,
            Instant asOf,
            List<StockPosition> locations) {
    }
}
