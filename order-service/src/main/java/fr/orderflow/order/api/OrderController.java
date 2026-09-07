package fr.orderflow.order.api;

import fr.orderflow.order.domain.OrderStatus;
import fr.orderflow.order.service.OrderService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Le {@code correlationId} entre par l'en-tete HTTP s'il est fourni, sinon
     * il est genere ici. C'est lui qui suivra la commande sur tout le parcours
     * evenementiel : c'est la base de la tracabilite (NF-04).
     */
    @PostMapping
    public ResponseEntity<OrderResponse> create(
            @Valid @RequestBody CreateOrderRequest request,
            @RequestHeader(value = "X-Correlation-Id", required = false) String correlationId) {

        String cid = correlationId != null ? correlationId : "corr-" + UUID.randomUUID();
        var order = orderService.createOrder(request, cid);
        return ResponseEntity.status(HttpStatus.CREATED)
                .header("X-Correlation-Id", cid)
                .body(OrderResponse.from(order));
    }

    @GetMapping("/{orderId}")
    public OrderResponse get(@PathVariable String orderId) {
        return OrderResponse.from(orderService.findById(orderId));
    }

    @GetMapping
    public List<OrderResponse> list(@RequestParam(required = false) OrderStatus status) {
        return orderService.findAll(status).stream().map(OrderResponse::from).toList();
    }
}
