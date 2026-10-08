import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generateur de charge et banc de chaos d'OrderFlow (phase 9). Java seul, aucune dependance :
 *
 * <pre>
 *   java scripts/OrderFlowLoad.java load  --orders 300 --concurrency 20
 *   java scripts/OrderFlowLoad.java chaos --orders 150 --rate 15 --project orderflow
 * </pre>
 * Lancer depuis la racine du depot (le mode chaos appelle {@code docker compose -p <projet> exec}).
 *
 * <p><b>load</b> : poste {@code --orders} commandes avec {@code --concurrency} requetes en parallele, puis
 * attend que toutes soient terminees et mesure latence HTTP, delai de bout en bout et debit. Sert a
 * comparer deux configurations (threads virtuels ou non).
 *
 * <p><b>chaos</b> : meme charge, pendant laquelle des conteneurs Docker du projet Compose
 * {@code --project} sont tues brutalement ({@code docker kill}, SIGKILL : aucune fermeture propre) puis
 * redemarres, y compris le broker. Verifie ensuite les invariants du systeme :
 * <ol>
 *   <li>toute commande acceptee (HTTP 201) atteint un etat terminal : rien n'est perdu (NF-01) ;</li>
 *   <li>le stock est coherent : disponible + reserve est constant, et le reserve augmente exactement de
 *       la quantite des commandes CONFIRMED (aucune reservation en double ni oubliee, NF-02/03) ;</li>
 *   <li>chaque commande CONFIRMED a un {@code payments.completed}, aucune commande CANCELLED n'en a ;</li>
 *   <li>aucun message de ce test dans un Dead Letter Topic.</li>
 * </ol>
 * Les doublons de <i>messages</i> sont normaux (livraison au moins une fois) : ils sont comptes et
 * rapportes, pas consideres comme une erreur. Ce qui compte, c'est l'absence de doublon d'<i>effet</i>.
 *
 * <p>Utilise la pile des ports par defaut (8081 order, 8082 inventory) et le broker du projet Compose.
 */
public class OrderFlowLoad {

    // --- configuration -------------------------------------------------------------------------
    private static final Map<String, String> OPTS = new HashMap<>();
    private static final String RUN_ID = Long.toString(System.currentTimeMillis(), 36);

    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3))
            .build();

    private static final Pattern ORDER = Pattern.compile("\\{\"orderId\":\"([^\"]+)\".*?\"status\":\"([A-Z_]+)\"");
    private static final Pattern ITEM = Pattern.compile("\"productId\":\"([^\"]+)\",\"quantity\":(\\d+)");
    private static final Pattern STOCK = Pattern.compile(
            "\\{\"productId\":\"([^\"]+)\",\"available\":(-?\\d+),\"reserved\":(-?\\d+)\\}");
    private static final Set<String> TERMINAL = Set.of("CONFIRMED", "CANCELLED");

    record Accepted(String orderId, String productId, int quantity, long acceptedAtNanos) {
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0 || !Set.of("load", "chaos").contains(args[0])) {
            System.err.println("Usage : java OrderFlowLoad.java <load|chaos> [--orders N] [--concurrency C] "
                    + "[--rate PAR_SECONDE] [--project NOM] [--chaos-interval S] [--downtime S] [--tail-kills N] [--timeout S] [--seed N]");
            System.exit(64);
        }
        boolean chaos = args[0].equals("chaos");
        for (int i = 1; i + 1 < args.length; i += 2) {
            OPTS.put(args[i].replaceFirst("^--", ""), args[i + 1]);
        }
        int orders = intOpt("orders", 200);
        int concurrency = intOpt("concurrency", 20);
        int timeoutSeconds = intOpt("timeout", 240);
        String project = OPTS.getOrDefault("project", "orderflow");
        String orderUrl = OPTS.getOrDefault("order-url", "http://localhost:8081");
        String inventoryUrl = OPTS.getOrDefault("inventory-url", "http://localhost:8082");

        System.out.printf("== OrderFlow %s : %d commandes, concurrence %d, run %s%n", args[0], orders, concurrency, RUN_ID);
        Map<String, int[]> stockBefore = stock(inventoryUrl);
        System.out.println("Stock initial : " + describe(stockBefore));

        // --- 1. charge (+ chaos) --------------------------------------------------------------
        List<Accepted> accepted = new CopyOnWriteArrayList<>();
        List<Long> httpLatenciesMs = new CopyOnWriteArrayList<>();
        AtomicInteger rejectedByClient = new AtomicInteger();
        AtomicBoolean loadDone = new AtomicBoolean();
        List<String> chaosLog = new CopyOnWriteArrayList<>();
        Thread chaosThread = null;
        if (chaos) {
            chaosThread = Thread.ofPlatform().name("chaos").start(() -> runChaos(project, loadDone, chaosLog));
        }

        long t0 = System.nanoTime();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Semaphore inFlight = new Semaphore(concurrency);
            Random random = new Random(intOpt("seed", 42));
            long paceMillis = intOpt("rate", 0) > 0 ? 1000L / intOpt("rate", 0) : 0;
            for (int i = 0; i < orders; i++) {
                String product = pickProduct(random);
                int n = i;
                if (paceMillis > 0) {
                    Thread.sleep(paceMillis);       // charge etalee : laisse le chaos chevaucher le traitement
                }
                inFlight.acquire();
                executor.submit(() -> {
                    try {
                        postOrder(orderUrl, "load-" + RUN_ID + "-" + n, product, accepted, httpLatenciesMs, rejectedByClient);
                    } finally {
                        inFlight.release();
                    }
                });
            }
        }
        double postSeconds = (System.nanoTime() - t0) / 1e9;
        loadDone.set(true);
        if (chaosThread != null) {
            chaosThread.join();
        }
        System.out.printf("Commandes acceptees (201) : %d / %d, refusees cote client apres 5 essais : %d, en %.1f s%n",
                accepted.size(), orders, rejectedByClient.get(), postSeconds);

        // --- 2. convergence : toutes les commandes acceptees deviennent terminales -----------
        Map<String, Long> terminalAtNanos = new ConcurrentHashMap<>();
        Map<String, String> finalStatus = new HashMap<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (System.nanoTime() < deadline) {
            Map<String, String> snapshot = statuses(orderUrl);
            long now = System.nanoTime();
            for (Accepted a : accepted) {
                String status = snapshot.get(a.orderId());
                if (status != null && TERMINAL.contains(status)) {
                    terminalAtNanos.putIfAbsent(a.orderId(), now);
                }
                finalStatus.put(a.orderId(), status == null ? "ABSENTE" : status);
            }
            if (terminalAtNanos.size() == accepted.size()) {
                break;
            }
            Thread.sleep(500);
        }
        double totalSeconds = (System.nanoTime() - t0) / 1e9;

        // --- 3. rapport de performance ------------------------------------------------------
        List<Long> e2e = new ArrayList<>();
        for (Accepted a : accepted) {
            Long done = terminalAtNanos.get(a.orderId());
            if (done != null) {
                e2e.add((done - a.acceptedAtNanos()) / 1_000_000);
            }
        }
        Collections.sort(httpLatenciesMs);
        Collections.sort(e2e);
        System.out.printf("Latence HTTP POST (ms)        : p50=%d p95=%d max=%d%n",
                pct(httpLatenciesMs, 50), pct(httpLatenciesMs, 95), pct(httpLatenciesMs, 100));
        System.out.printf("Delai bout en bout (ms)       : p50=%d p95=%d max=%d  (acceptee -> CONFIRMED/CANCELLED, resolution ~0,5 s)%n",
                pct(e2e, 50), pct(e2e, 95), pct(e2e, 100));
        System.out.printf("Debit bout en bout            : %.1f commandes/s  (%d terminees en %.1f s)%n",
                terminalAtNanos.size() / totalSeconds, terminalAtNanos.size(), totalSeconds);

        // --- 4. invariants ----------------------------------------------------------------
        boolean ok = true;
        Map<String, Integer> byStatus = new TreeMap<>();
        finalStatus.values().forEach(s -> byStatus.merge(s, 1, Integer::sum));
        System.out.println("Statuts finaux                : " + byStatus);
        long notTerminal = accepted.size() - terminalAtNanos.size();
        ok &= check("1. aucune commande acceptee n'est perdue ni bloquee", notTerminal == 0,
                notTerminal + " commande(s) non terminale(s)");

        // le stock converge apres les compensations asynchrones (orders.cancelled -> inventory)
        Map<String, Integer> confirmedQty = new HashMap<>();
        for (Accepted a : accepted) {
            if ("CONFIRMED".equals(finalStatus.get(a.orderId()))) {
                confirmedQty.merge(a.productId(), a.quantity(), Integer::sum);
            }
        }
        String stockProblem = "";
        long stockDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
        Map<String, int[]> stockAfter;
        do {
            stockAfter = stock(inventoryUrl);
            stockProblem = stockProblem(stockBefore, stockAfter, confirmedQty);
            if (stockProblem.isEmpty()) {
                break;
            }
            Thread.sleep(1000);
        } while (System.nanoTime() < stockDeadline);
        System.out.println("Stock final                   : " + describe(stockAfter));
        ok &= check("2. stock coherent (disponible + reserve constant, reserve = commandes CONFIRMED)",
                stockProblem.isEmpty(), stockProblem);

        if (chaos) {
            Set<String> ours = new HashSet<>();
            accepted.forEach(a -> ours.add(a.orderId()));
            Map<String, Integer> paymentMessages = keyCounts(project, "payments.completed", ours);
            Set<String> confirmed = new HashSet<>();
            Set<String> cancelled = new HashSet<>();
            finalStatus.forEach((id, s) -> {
                if (s.equals("CONFIRMED")) {
                    confirmed.add(id);
                } else if (s.equals("CANCELLED")) {
                    cancelled.add(id);
                }
            });
            Set<String> missing = new HashSet<>(confirmed);
            missing.removeAll(paymentMessages.keySet());
            Set<String> inconsistent = new HashSet<>(cancelled);
            inconsistent.retainAll(paymentMessages.keySet());
            ok &= check("3. chaque CONFIRMED a un payments.completed, aucune CANCELLED n'en a",
                    missing.isEmpty() && inconsistent.isEmpty(),
                    missing.size() + " CONFIRMED sans paiement, " + inconsistent.size() + " CANCELLED avec paiement");
            int duplicates = paymentMessages.values().stream().mapToInt(c -> c - 1).sum();
            System.out.printf("   (info) messages payments.completed en double : %d (normal en livraison au moins une fois)%n",
                    duplicates);

            int deadLetters = 0;
            for (String topic : List.of("orders.created.DLT", "orders.cancelled.DLT", "orders.confirmed.DLT",
                    "inventory.reserved.DLT", "inventory.rejected.DLT", "payments.completed.DLT", "payments.failed.DLT")) {
                deadLetters += keyCounts(project, topic, ours).values().stream().mapToInt(Integer::intValue).sum();
            }
            ok &= check("4. aucun message de ce test dans un Dead Letter Topic", deadLetters == 0,
                    deadLetters + " message(s) en DLT");
            System.out.println("Chaos applique :");
            chaosLog.forEach(line -> System.out.println("   " + line));
        }
        System.out.println(ok ? "RESULTAT : OK" : "RESULTAT : ECHEC");
        System.exit(ok ? 0 : 1);
    }

    // --- charge -------------------------------------------------------------------------------

    /**
     * 80 % de commandes qui aboutissent (sku-003, stock 500), 10 % sku-002, 10 % sku-005 (1 250 EUR :
     * refus de paiement, donc compensation, et stock de 2 seulement : rejets de stock aussi).
     */
    private static String pickProduct(Random random) {
        int r = random.nextInt(10);
        return r < 8 ? "sku-003" : r < 9 ? "sku-002" : "sku-005";
    }

    private static void postOrder(String orderUrl, String correlationId, String product, List<Accepted> accepted,
                                  List<Long> latenciesMs, AtomicInteger rejected) {
        String body = "{\"customerId\":\"cust-load\",\"items\":[{\"productId\":\"" + product + "\",\"quantity\":1}]}";
        for (int attempt = 1; attempt <= 5; attempt++) {
            long start = System.nanoTime();
            try {
                HttpRequest request = HttpRequest.newBuilder(URI.create(orderUrl + "/api/orders"))
                        .timeout(Duration.ofSeconds(10))
                        .header("Content-Type", "application/json")
                        .header("X-Correlation-Id", correlationId)
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
                HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                if (response.statusCode() == 201) {
                    Matcher m = Pattern.compile("\"orderId\":\"([^\"]+)\"").matcher(response.body());
                    if (m.find()) {
                        latenciesMs.add(elapsedMs);
                        accepted.add(new Accepted(m.group(1), product, 1, System.nanoTime()));
                        return;
                    }
                }
            } catch (IOException e) {
                // service tue ou en cours de redemarrage : on reessaie
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            try {
                Thread.sleep(1000L * attempt);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        rejected.incrementAndGet();
    }

    // --- lecture de l'etat --------------------------------------------------------------------

    private static Map<String, String> statuses(String orderUrl) {
        Map<String, String> result = new HashMap<>();
        try {
            String json = get(orderUrl + "/api/orders");
            Matcher m = ORDER.matcher(json);
            while (m.find()) {
                result.put(m.group(1), m.group(2));
            }
        } catch (Exception e) {
            // le service redemarre : on reessaiera au tour suivant
        }
        return result;
    }

    /**
     * productId -> {disponible, reserve}.
     */
    private static Map<String, int[]> stock(String inventoryUrl) throws Exception {
        Map<String, int[]> result = new TreeMap<>();
        Matcher m = STOCK.matcher(getWithRetry(inventoryUrl + "/api/stock"));
        while (m.find()) {
            result.put(m.group(1), new int[] {Integer.parseInt(m.group(2)), Integer.parseInt(m.group(3))});
        }
        return result;
    }

    private static String stockProblem(Map<String, int[]> before, Map<String, int[]> after, Map<String, Integer> confirmedQty) {
        StringBuilder problems = new StringBuilder();
        for (var entry : before.entrySet()) {
            String product = entry.getKey();
            int[] b = entry.getValue();
            int[] a = after.get(product);
            int expectedReservedDelta = confirmedQty.getOrDefault(product, 0);
            if (a == null) {
                problems.append(product).append(" disparu; ");
                continue;
            }
            if (a[0] + a[1] != b[0] + b[1]) {
                problems.append(product).append(" total change ").append(b[0] + b[1]).append("->").append(a[0] + a[1]).append("; ");
            }
            if (a[0] < 0) {
                problems.append(product).append(" disponible negatif ").append(a[0]).append("; ");
            }
            if (a[1] - b[1] != expectedReservedDelta) {
                problems.append(product).append(" reserve +").append(a[1] - b[1]).append(" au lieu de +")
                        .append(expectedReservedDelta).append("; ");
            }
        }
        return problems.toString();
    }

    private static String describe(Map<String, int[]> stock) {
        StringBuilder sb = new StringBuilder();
        stock.forEach((k, v) -> sb.append(k).append("=").append(v[0]).append("/").append(v[1]).append(" "));
        return sb.append("(disponible/reserve)").toString();
    }

    // --- chaos --------------------------------------------------------------------------------

    private static void runChaos(String project, AtomicBoolean loadDone, List<String> log) {
        List<String> targets = List.of("order-service", "inventory-service", "payment-service", "kafka",
                "notification-service", "inventory-service", "order-service", "payment-service");
        int intervalSeconds = intOpt("chaos-interval", 6);
        int downtimeSeconds = intOpt("downtime", 3);
        int tailKills = intOpt("tail-kills", 2);        // arrets supplementaires apres la fin de la charge
        int index = 0;
        int killsAfterLoad = 0;
        long started = System.nanoTime();
        try {
            Thread.sleep(2000);
            while (!loadDone.get() || killsAfterLoad < tailKills) {
                if (loadDone.get()) {
                    killsAfterLoad++;
                }
                String service = targets.get(index++ % targets.size());
                String container = project + "-" + service + "-1";
                double at = (System.nanoTime() - started) / 1e9;
                docker("kill", container);
                log.add(String.format("t=%.0fs  docker kill %s (SIGKILL), arret de %d s", at, container, downtimeSeconds));
                Thread.sleep(downtimeSeconds * 1000L);
                docker("start", container);
                Thread.sleep(intervalSeconds * 1000L);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // quoi qu'il arrive, tout doit tourner pour la phase de verification
            for (String service : List.of("kafka", "order-service", "inventory-service", "payment-service", "notification-service")) {
                docker("start", project + "-" + service + "-1");
            }
        }
    }

    private static String docker(String... args) {
        List<String> command = new ArrayList<>(List.of("docker"));
        command.addAll(List.of(args));
        return run(command, 30);
    }

    /**
     * Cles (restreintes a {@code only}) d'un topic et nombre de messages par cle, lus depuis le debut
     * avec le consumer console du broker.
     */
    private static Map<String, Integer> keyCounts(String project, String topic, Set<String> only) {
        String out = run(List.of("docker", "compose", "-p", project, "exec", "-T", "kafka",
                "/opt/kafka/bin/kafka-console-consumer.sh", "--bootstrap-server", "localhost:9092", "--topic", topic,
                "--from-beginning", "--timeout-ms", "6000", "--property", "print.key=true"), 60);
        Map<String, Integer> counts = new HashMap<>();
        for (String line : out.split("\\R")) {
            int tab = line.indexOf('\t');
            String key = tab > 0 ? line.substring(0, tab) : line.trim();
            if (only.contains(key)) {
                counts.merge(key, 1, Integer::sum);
            }
        }
        return counts;
    }

    // --- utilitaires --------------------------------------------------------------------------

    private static String run(List<String> command, int timeoutSeconds) {
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            return output;
        } catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return "";
        }
    }

    private static String get(String url) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build();
        return HTTP.send(request, HttpResponse.BodyHandlers.ofString()).body();
    }

    private static String getWithRetry(String url) throws Exception {
        Exception last = null;
        for (int i = 0; i < 30; i++) {
            try {
                return get(url);
            } catch (IOException e) {
                last = e;
                Thread.sleep(1000);
            }
        }
        throw last;
    }

    private static long pct(List<Long> sorted, int percentile) {
        if (sorted.isEmpty()) {
            return -1;
        }
        int index = Math.min(sorted.size() - 1, (int) Math.ceil(percentile / 100.0 * sorted.size()) - 1);
        return sorted.get(Math.max(index, 0));
    }

    private static boolean check(String label, boolean condition, String detail) {
        System.out.printf("[%s] %s%s%n", condition ? "OK" : "KO", label, condition || detail.isEmpty() ? "" : " : " + detail);
        return condition;
    }

    private static int intOpt(String name, int defaultValue) {
        return Integer.parseInt(OPTS.getOrDefault(name, Integer.toString(defaultValue)));
    }
}
