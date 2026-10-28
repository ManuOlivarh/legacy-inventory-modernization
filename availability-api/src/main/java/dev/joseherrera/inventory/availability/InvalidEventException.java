package dev.joseherrera.inventory.availability;

/** The record can never be applied, however often it is retried: it goes straight to the DLQ. */
class InvalidEventException extends RuntimeException {

    InvalidEventException(String message) {
        super(message);
    }

    InvalidEventException(String message, Throwable cause) {
        super(message, cause);
    }
}
