package dev.joseherrera.inventory.simulator;

public enum MovementType {
    SALE("VT"),
    RETURN("DV"),
    RECEIPT("RC"),
    ADJUSTMENT_IN("AP"),
    ADJUSTMENT_OUT("AN");

    private final String legacyCode;

    MovementType(String legacyCode) {
        this.legacyCode = legacyCode;
    }

    public String legacyCode() {
        return legacyCode;
    }
}
