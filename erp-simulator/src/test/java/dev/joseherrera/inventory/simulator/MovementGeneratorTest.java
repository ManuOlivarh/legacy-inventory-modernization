package dev.joseherrera.inventory.simulator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;

class MovementGeneratorTest {

    private final MovementGenerator generator =
            new MovementGenerator(List.of("A1", "A2"), List.of("C001", "T001"), new SplittableRandom(42));

    @Test
    void quantitiesStayWithinRealisticRanges() {
        IntStream.range(0, 10_000).mapToObj(i -> generator.next()).forEach(m -> {
            switch (m.type()) {
                case SALE -> assertThat(m.quantity()).isBetween(1, 3);
                case RETURN -> assertThat(m.quantity()).isEqualTo(1);
                case RECEIPT -> assertThat(m.quantity()).isBetween(10, 50);
                case ADJUSTMENT_IN, ADJUSTMENT_OUT -> assertThat(m.quantity()).isBetween(1, 5);
            }
        });
    }

    @Test
    void salesDominateTheMix() {
        Map<MovementType, Integer> counts = new EnumMap<>(MovementType.class);
        IntStream.range(0, 10_000).forEach(i -> counts.merge(generator.next().type(), 1, Integer::sum));

        assertThat(counts.get(MovementType.SALE)).isBetween(6_700, 7_300);
        assertThat(counts).containsOnlyKeys(MovementType.values());
    }

    @Test
    void referencesAreUniqueAndFitTheLegacyColumn() {
        var refs = IntStream.range(0, 1_000).mapToObj(i -> generator.next().reference()).toList();

        assertThat(refs).doesNotHaveDuplicates();
        assertThat(refs).allSatisfy(ref -> assertThat(ref).hasSizeLessThanOrEqualTo(20));
    }

    @Test
    void refusesAnEmptyCatalog() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new MovementGenerator(List.of(), List.of("C001"), new SplittableRandom()));
    }
}
