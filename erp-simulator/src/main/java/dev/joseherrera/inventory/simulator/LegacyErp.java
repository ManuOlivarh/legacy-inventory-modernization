package dev.joseherrera.inventory.simulator;

import java.util.List;

import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;

/** Talks to the legacy ERP exactly as its own application does: through the stored procedure. */
@Component
class LegacyErp {

    private static final String USER = "SIMULADOR";

    private final JdbcClient jdbc;

    LegacyErp(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    List<String> activeArticles() {
        return jdbc.sql("SELECT ART_COD FROM dbo.INV_ART WHERE ART_EST = 'A'").query(String.class).list();
    }

    List<String> locations() {
        return jdbc.sql("SELECT BOD_COD FROM dbo.INV_BOD").query(String.class).list();
    }

    void record(Movement movement) {
        jdbc.sql("EXEC dbo.SP_INV_MOVIMIENTO ?, ?, ?, ?, ?, ?")
                .params(movement.article(), movement.location(), movement.type().legacyCode(),
                        movement.quantity(), USER, movement.reference())
                .update();
    }
}
