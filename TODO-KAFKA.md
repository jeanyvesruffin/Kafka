# TODO Kafka — ce qu'il te reste à implémenter

Le squelette livré tourne **de bout en bout sans broker** : la logique métier, la persistance, l'outbox, l'idempotence et les compensations sont écrites et testées. Seule la couche transport est absente.

Tout ce que tu as à écrire tient dans **cinq packages `messaging`**, aujourd'hui vides. Chacun contient un `package-info.java` qui détaille précisément quoi y mettre, avec des exemples de code.

---

## Le point d'extension unique

```text
fr.orderflow.common.messaging.EventPublisher         ← l'interface (1 méthode)
fr.orderflow.common.messaging.LoggingEventPublisher  ← l'implémentation actuelle
fr.orderflow.common.config.MessagingConfig           ← qui choisit laquelle
```

La bascule tient en deux gestes :

1. écrire un `KafkaEventPublisher implements EventPublisher` annoté `@Component` ;
2. mettre `orderflow.messaging.publisher: kafka` dans l'`application.yml` du service.

Le publisher de log n'est alors plus créé, et **le reste du projet se met à publier pour de vrai sans qu'une seule autre ligne ne bouge.**

C'est le test de la qualité du découplage : si tu dois modifier autre chose que ce bean pour brancher Kafka, quelque chose fuit.

> Le choix passe par une propriété et non par `@ConditionalOnMissingBean` sur un `@Component` : cette annotation est évaluée pendant le scan de composants, dans un ordre non garanti, et donnerait un résultat instable selon le nom de tes packages. Elle n'est fiable que dans une classe d'auto-configuration.

---

## Étapes

### Démarrage

Deux modes, détaillés dans la rubrique [Démarrage du README](README.md#démarrage). Java 25 dans les deux cas.

<details>
<summary>Démarrage avec droits administrateur (Docker)</summary>

```shell
docker compose up -d --build      # Kafka + AKHQ + les 4 services
docker compose up -d kafka akhq   # ou infra seule, services lancés depuis l'IDE
docker compose logs -f            # suivre les logs
docker compose down               # arrêter (down -v efface aussi Kafka et les bases H2)
```

AKHQ : `http://localhost:8090`. Kafka reste joignable depuis le poste sur `localhost:9092`.

</details>

<details>
<summary>Démarrage sans droits administrateur</summary>

- Les différents modules se démarrent à l'aide des commandes ci-dessous :

  ```shell
  mvn clean install # à la racine du module désiré
  mvn spring-boot:run
  ```

- Le moteur Kafka s'exécute à l'aide de la commande ci-dessous :

  ```shell
  bin\windows\kafka-server-start.bat config\server.properties # à la racine de votre installation Kafka
  ```

- Démarrage AKHQ :

  ```shell
  java -Dmicronaut.config.files=application.yml -jar akhq-0.28.0-all.jar # à la racine de votre installation AKHQ (Java 25 requis)
  ```

</details>

<a id="phase-1"></a>

### Phase 1 — Premier flux Order → Inventory

- [x] Décommenter `spring-boot-starter-kafka` dans `order-service/pom.xml` et `inventory-service/pom.xml`
- [x] Décommenter le bloc `spring.kafka` dans `order-service/src/main/resources/application.yml`
- [x] Écrire `fr.orderflow.order.messaging.KafkaEventOrderPublisher` et `fr.orderflow.inventory.messaging.KafkaEventInventoryPublisher`, qui doivent implémenter `EventPublisher`
- [x] Basculer `orderflow.messaging.publisher` sur `kafka` dans les `application.yml` concernés
- [x] Déclarer et configurer les topics via des beans `NewTopic` (⚠️ `replicationFactor = 1` en local mono-nœud) dans les fichiers `fr.orderflow.inventory.messaging.KafkaTopicsInventoryConfig` et `fr.orderflow.order.messaging.KafkaTopicsOrderConfig`
- [x] Écrire `fr.orderflow.inventory.messaging.OrderEventListener` → appelle `inventoryService.handleOrderCreated(...)` et `inventoryService.handleOrderCancelled(...)`.
- [x] Écrire `fr.orderflow.order.messaging.InventoryEventListener` → appelle `orderService.onInventoryReserved(...)` et `orderService.onInventoryRejected(...)`.

**Validé quand** : un `POST /api/orders` fait bouger le stock dans `GET /api/stock`, et que tu vois les messages passer dans AKHQ.

<a id="phase-2"></a>

### Phase 2 — Payment + saga complète

- [x] `fr.orderflow.payment.messaging.InventoryEventListener` → `paymentService.handleInventoryReserved(...)`
- [x] `fr.orderflow.order.messaging.*Listener` pour les 4 topics consommés par order-service
- [x] `fr.orderflow.notification.messaging.*Listener` pour les 2 topics terminaux
- [x] `fr.orderflow.payment.messaging.KafkaEventPaymentPublisher` (sans lui, payment-service ne démarre pas) et `KafkaTopicsPaymentConfig` pour les topics `payments.*`
- [x] Compensation limitée aux réservations de la commande annulée (`stock_reservation`) : annuler une commande rejetée ne libère plus le stock réservé par les autres

**Validé quand** : commander `sku-001 x2` aboutit à `CONFIRMED`, et commander `sku-005 x1` (1250,00 € > plafond) aboutit à `CANCELLED` **avec le stock libéré**.

<a id="phase-3"></a>

### Phase 3 — Vérifier l'outbox et l'idempotence sous Kafka

- [x] Arrêter le broker, poster 3 commandes, le redémarrer → les 3 événements doivent partir (en mode Docker : `docker compose stop kafka`, puis `docker compose start kafka`)
- [x] Rejouer manuellement un message depuis AKHQ → le stock ne doit pas bouger deux fois

Rien à coder ici : les garanties sont déjà en place. L'exercice est de **prouver qu'elles tiennent** face à un vrai broker.

<a id="phase-4"></a>

### Phase 4 — Retry et Dead Letter Topic

- [x] `ErrorHandlingDeserializer` sur tous les consumers
- [x] `DefaultErrorHandler` ou `@RetryableTopic` avec backoff exponentiel
- [x] Classer les exceptions : `DeserializationException` → DLT immédiat, `OptimisticLockingFailureException` → retryable
- [x] Injecter une panne transitoire dans `PaymentGatewaySimulator` pour observer les retries

**Validé quand** : un payload corrompu finit dans la DLT sans bloquer les autres messages, et une panne transitoire du paiement se résout après passage par `inventory.reserved-retry-0` et `-retry-1`. Détail : [README, phase 4](README.md#phase-4--retry-et-dead-letter-topic).

<a id="phase-5"></a>

### Phase 5 — Observabilité

- [x] Propager le `correlationId` de l'en-tête Kafka vers le MDC
- [x] Vérifier le lag des consumer groups via `/actuator/metrics` et AKHQ
- [x] Brancher le tracing (l'instrumentation Kafka est automatique en Boot 4.1)

**Validé quand** : une commande est traçable de bout en bout via son `correlationId` dans les logs des services. Détail : [README, phase 5](README.md#phase-5--observabilité).

> L'instrumentation Kafka n'est « automatique » qu'une fois activée : `spring.kafka.template.observation-enabled` et `spring.kafka.listener.observation-enabled` valent `false` par défaut.

<a id="phase-7"></a>

### Phase 7 — Avro

- [x] `avro-maven-plugin`, schémas `.avsc` dans `orderflow-common`
- [x] Remplacer `EventSerializer` par une variante Avro (pour `orders.created`, topic migré ; les six autres restent en JSON)
- [x] Ajouter un champ optionnel et vérifier la compatibilité `BACKWARD`

**Validé quand** : un consommateur à jour lit les messages d'un producteur resté sur l'ancien schéma (et inversement). Plan B retenu (sans registre de schémas). Détail : [README, phase 7](README.md#phase-7--avro).

<a id="phase-8"></a>

### Phase 8 — Kafka Streams (bonus)

- [x] Service `analytics-service` (`:8085`) : commandes par statut et chiffre d'affaires en temps réel
- [x] Agrégats **idempotents** (KTable par commande), indépendants de l'ordre d'arrivée entre topics
- [x] Interrogation des state stores par API REST (`/api/analytics/*`)
- [x] Message illisible → DLQ au lieu d'arrêter l'application
- [x] Compteurs identiques après rejeu du topic depuis le début

Détail : [README, phase 8](README.md#phase-8--kafka-streams-bonus).

<a id="phase-9"></a>

### Phase 9 — Exactly-once, chaos testing, virtual threads (bonus)

- [x] Exactly-once transactionnel (`exactly_once_v2`) pour le flux Kafka → Kafka d'Analytics
- [x] Chaos testing : arrêts brutaux (`SIGKILL`) des services et du broker pendant une charge, invariants vérifiés
- [x] Virtual threads : mesure avant / après

Détail : [README, phase 9](README.md#phase-9--exactly-once-chaos-testing-virtual-threads-bonus).

---

## Ce que le squelette te donne déjà, ne le réécris pas

| Besoin                   | Où c'est déjà fait                                                   |
| ------------------------ | -------------------------------------------------------------------- |
| Contrats d'événements    | `orderflow-common` — `sealed interface OrderFlowEvent` + 7 records   |
| Noms de topics           | `Topics` — aucune chaîne en dur ailleurs                             |
| Sérialisation JSON       | `EventSerializer` (Jackson 3 / `JsonMapper`)                         |
| Outbox transactionnel    | `OutboxEventEntity` + `OutboxRelay`                                  |
| Idempotence consumer     | `ProcessedEventEntity` (inventory, payment)                          |
| Machine à états commande | `OrderEntity.transitionTo(...)` — refuse de quitter un état terminal |
| Compensation stock       | `InventoryService.handleOrderCancelled(...)`                         |
| Verrouillage concurrent  | `@Version` sur `StockEntity`                                         |

**Règle** : un listener ne contient jamais de logique métier. Il désérialise et délègue. Si tu ressens le besoin d'un `if` métier dans un listener, c'est qu'une méthode manque dans le service.
