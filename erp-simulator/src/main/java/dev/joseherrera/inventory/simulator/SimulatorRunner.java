package dev.joseherrera.inventory.simulator;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.random.RandomGenerator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.dao.DataAccessException;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

@Component
class SimulatorRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(SimulatorRunner.class);

    private final LegacyErp erp;
    private final SimulatorProperties properties;
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private long recorded;

    SimulatorRunner(LegacyErp erp, SimulatorProperties properties) {
        this.erp = erp;
        this.properties = properties;
    }

    @Override
    public void run(ApplicationArguments args) {
        var generator = new MovementGenerator(erp.activeArticles(), erp.locations(), RandomGenerator.getDefault());
        long periodNanos = (long) (TimeUnit.SECONDS.toNanos(1) / properties.movementsPerSecond());
        scheduler.scheduleAtFixedRate(() -> tick(generator), 0, periodNanos, TimeUnit.NANOSECONDS);
        log.info("Simulating {} movements/s against the legacy ERP", properties.movementsPerSecond());
    }

    private void tick(MovementGenerator generator) {
        try {
            erp.record(generator.next());
            if (++recorded % 100 == 0) {
                log.info("{} movements recorded", recorded);
            }
        } catch (DataAccessException e) {
            log.warn("Movement rejected by the legacy ERP: {}", e.getMostSpecificCause().getMessage());
        }
    }

    @PreDestroy
    void stop() throws InterruptedException {
        scheduler.shutdown();
        scheduler.awaitTermination(5, TimeUnit.SECONDS);
    }
}
