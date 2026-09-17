package dev.joseherrera.inventory.simulator;

/** Movement types as the legacy ERP encodes them in INV_MOV.MOV_TIP. */
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
