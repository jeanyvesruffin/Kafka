package fr.orderflow.order.service;

public class OrderNotFoundException extends RuntimeException {

    public OrderNotFoundException(String orderId) {
        super("Commande introuvable : " + orderId);
    }
}
