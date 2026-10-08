package fr.orderflow.order.api;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * @param couponCode code promotionnel, optionnel : transporte tel quel dans {@code OrderCreated}
 *                   (champ optionnel ajoute en version 2 du schema Avro, phase 7)
 */
public record CreateOrderRequest(
        @NotBlank String customerId,
        @NotEmpty @Valid List<Item> items,
        @Size(max = 32) String couponCode) {

    public CreateOrderRequest(String customerId, List<Item> items) {
        this(customerId, items, null);
    }

    public record Item(
            @NotBlank String productId,
            @Min(1) int quantity) {
    }
}
