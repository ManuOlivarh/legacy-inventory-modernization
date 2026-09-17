package dev.joseherrera.inventory.simulator;

public record Movement(String article, String location, MovementType type, int quantity, String reference) {
}
