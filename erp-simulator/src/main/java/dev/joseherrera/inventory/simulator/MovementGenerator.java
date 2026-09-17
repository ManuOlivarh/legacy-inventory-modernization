package dev.joseherrera.inventory.simulator;

import java.util.List;
import java.util.random.RandomGenerator;

/**
 * Produces a plausible mix of store and warehouse activity: mostly sales, some
 * receipts, and a trickle of returns and stock-count adjustments.
 */
public final class MovementGenerator {

    private final List<String> articles;
    private final List<String> locations;
    private final RandomGenerator random;
    private long sequence;

    public MovementGenerator(List<String> articles, List<String> locations, RandomGenerator random) {
        if (articles.isEmpty() || locations.isEmpty()) {
            throw new IllegalArgumentException("the legacy ERP has no articles or locations to simulate");
        }
        this.articles = List.copyOf(articles);
        this.locations = List.copyOf(locations);
        this.random = random;
    }

    public Movement next() {
        MovementType type = nextType();
        return new Movement(
                articles.get(random.nextInt(articles.size())),
                locations.get(random.nextInt(locations.size())),
                type,
                quantityFor(type),
                "SIM%08d".formatted(++sequence));
    }

    private MovementType nextType() {
        int roll = random.nextInt(100);
        if (roll < 70) return MovementType.SALE;
        if (roll < 75) return MovementType.RETURN;
        if (roll < 90) return MovementType.RECEIPT;
        if (roll < 95) return MovementType.ADJUSTMENT_IN;
        return MovementType.ADJUSTMENT_OUT;
    }

    private int quantityFor(MovementType type) {
        return switch (type) {
            case SALE -> random.nextInt(1, 4);
            case RETURN -> 1;
            case RECEIPT -> random.nextInt(10, 51);
            case ADJUSTMENT_IN, ADJUSTMENT_OUT -> random.nextInt(1, 6);
        };
    }
}
