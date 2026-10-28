package dev.joseherrera.inventory.translator;

import java.math.BigDecimal;

import dev.joseherrera.inventory.events.MovementReason;

/** Translates INV_MOV.MOV_TIP, where the direction of the movement lives. */
enum MovementReasons {

    VT(MovementReason.SALE, -1),
    DV(MovementReason.RETURN, 1),
    RC(MovementReason.RECEIPT, 1),
    AP(MovementReason.ADJUSTMENT_IN, 1),
    AN(MovementReason.ADJUSTMENT_OUT, -1);

    private final MovementReason reason;
    private final int sign;

    MovementReasons(MovementReason reason, int sign) {
        this.reason = reason;
        this.sign = sign;
    }

    static boolean isKnown(String legacyCode) {
        for (MovementReasons value : values()) {
            if (value.name().equals(legacyCode)) {
                return true;
            }
        }
        return false;
    }

    static MovementReasons of(String legacyCode) {
        try {
            return valueOf(legacyCode);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("unknown legacy movement type: " + legacyCode, e);
        }
    }

    MovementReason reason() {
        return reason;
    }

    /** The legacy quantity is always positive; the sign comes from the type. */
    BigDecimal signed(BigDecimal quantity) {
        return quantity.abs().multiply(BigDecimal.valueOf(sign));
    }
}
