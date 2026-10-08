package fr.orderflow.order.api;

import fr.orderflow.order.domain.OrderStatus;
import fr.orderflow.order.service.OrderService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/orders")
@RequiredArgsConstructor
public class OrderController {

    private final OrderService orderService;

    /**
     * Le {@code correlationId} entre par l'en-tete HTTP {@code X-Correlation-Id} s'il est fourni et
     * acceptable, sinon il est genere. {@link CorrelationIdFilter} s'en charge, le met dans le MDC des
     * logs et le renvoie dans la reponse. C'est lui qui suivra la commande sur tout le parcours
     * evenementiel : c'est la base de la tracabilite (NF-04).
     */
    @PostMapping
    public ResponseEntity<OrderResponse> create(
            @Valid @RequestBody CreateOrderRequest request,
            @RequestAttribute(CorrelationIdFilter.REQUEST_ATTRIBUTE) String cid) {

        var order = orderService.createOrder(request, cid);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(OrderResponse.from(order));
    }

    @GetMapping("/{orderId}")
    public OrderResponse get(@PathVariable String orderId) {
        return OrderResponse.from(orderService.findById(orderId));
    }

    @GetMapping
    public List<OrderResponse> list(@RequestParam(required = false) OrderStatus status) {
        return orderService.findAll(status)
                .stream()
                .map(OrderResponse::from)
                .toList();
    }
}
