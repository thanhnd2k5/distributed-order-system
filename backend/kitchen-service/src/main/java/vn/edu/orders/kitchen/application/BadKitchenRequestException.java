package vn.edu.orders.kitchen.application;

public class BadKitchenRequestException extends RuntimeException {
    public BadKitchenRequestException(String message) {
        super(message);
    }
}
