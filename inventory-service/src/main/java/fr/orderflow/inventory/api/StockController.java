package fr.orderflow.inventory.api;

import fr.orderflow.inventory.service.InventoryService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * Endpoint de lecture du stock — outil de debug, pas une API metier.
 * Bien pratique pour verifier de visu l'effet d'une reservation ou d'une
 * compensation pendant les tests manuels.
 */
@RestController
@RequestMapping("/api/stock")
public class StockController {

    private final InventoryService inventoryService;

    public StockController(InventoryService inventoryService) {
        this.inventoryService = inventoryService;
    }

    @GetMapping
    public List<StockView> list() {
        return inventoryService.findAllStock()
                .stream()
                .map(s -> new StockView(s.getProductId(), s.getQuantityAvailable(), s.getQuantityReserved()))
                .toList();
    }

    public record StockView(String productId, int available, int reserved) {
    }
}
