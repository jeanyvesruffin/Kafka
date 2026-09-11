# Kafka

<details>
<summary>Définitions & concepts</summary>

Voici le contenu masqué par défaut.
Tu peux y mettre du **texte**, des *listes* ou du `code`.

* `Event`:
    * Description d'une action (ex métier: passage d'une commande, d'un payement, via un site e-commerce)
    * A diffuser à un ou plusieurs microservices
    * Stockés sous forme de messages
    * Stockés dans une couche logique nommée topics
* `Event Streaming` : Diffusion d'événements en continu
* `Producer` (écriture)
    * Application cliente qui écrit des données
    * Dans un ou des topics
* `Consumer` (lecture)
    * Application client qui souscrit à des topics
* `Asynchrone`
    * Découplage entre l'activité du/des producers
    * Découplage entre l'activité et du/des consumers
* `Topics`
    * Une couche logique de stockage des messages
    * Permettant de lire et de relire les messages
    * Les messages ne sont pas détruits
* `Commit log`
    * Méthode employée par Kafka pour stocker les messages
    * Ordonnée, séquentiel et jamais détruit lors de leur consommation
* `Offset`
    * Caractéristiques : où commencer (plus tôt ou plus tard)
    * Important pour attester de la bonne délivrance du message
* `Partition`
    * Fragmentation logique des topics en morceau
    * Permet de distribuer sur l'ensemble d'un même cluster
    * En les conservant dans le même topic
    * Important pour la performance et la scalabilité

_Exemple de cluster partitionné_

![Topic](Docs/Topic.png)

* `Clef de partition`
    * Information utilisée pour déterminer où stocker
    * Défini dans quelle partition
    * Fonction de hachage
* `Segment` (partition découpé)
    * au sein des partitions
    * regroupement de messages pour un stockage physique
    * veleur par défaut = 1GB
* `Réplication`
    * Les partitions sont dupliquées (haute disponibilité)
    * Sur un ou plusieurs autre serveurs (brokers)
    * Leader vs Follower (Broker principal ou secondaire)

Exemple de cluster partitionné avec Replicat

![Replicat](Docs/Replicat.png)

* `Consumer Group`, à la différence des producers dont le travail est plus simple :
    * Les consumers doivent s'organiser pour consommer les partitions
    * Potentiellement de différents topics
    * Un des brokers à en charge de coordonner (coordinator)
        * coordinator est en charge de l'offset

## Installation

### Zookeeper (plus nécessaire remplacé par KRaft)

### Kafka & Kafka manager (CMAK)

* Télécharger Kafka [quickstart](https://kafka.apache.org/43/getting-started/quickstart/)
* Décompresser le sous C:/ (Attention ne fonctionne pas si le chemin est trop long)
* Exécuter les commande dans un terminal Windows command prompt

```sh
for /f "tokens=*" %i in ('bin\windows\kafka-storage.bat random-uuid 2^>nul') do set KAFKA_CLUSTER_ID=%i
set KAFKA_CLUSTER_ID=random-uuid retourné précedemment
# Vérification
echo %KAFKA_CLUSTER_ID%
# Retourne random-uuid retourné précedemment
bin\windows\kafka-storage.bat format --standalone -t %KAFKA_CLUSTER_ID% -c config\server.properties
# doit retourner Formatting dynamic metadata voter directory /tmp/kraft-combined-logs with metadata.version 4.3-IV0.
```

* Configuration cluster et noeuf Kafka. Ouvrir pour modifier/ contrôler la configuration kafka dans le fichier
  `config/server.properties` ou (et) `controller.properties` ou (et) `broker.properties`
  (Exemple : [server.properties](server.properties))):

| Paramètre                                                 | Définition                                                                                                                                                                                                                                                                                                                                      | Type    | Défaut                                           |
|-----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------|--------------------------------------------------|
| `log.dirs`                                                | Liste de répertoires (séparés par des virgules) où sont stockées les données de log. Si absent, la valeur de `log.dir` est utilisée.                                                                                                                                                                                                            | list    | `null` (repli sur `log.dir` = `/tmp/kafka-logs`) |
| `num.partitions`                                          | Nombre par défaut de partitions par topic. S'applique à la création automatique de topics, à la création de topics internes de Kafka Streams, et à `AdminClient#createTopics` quand le nombre de partitions vaut -1.                                                                                                                            | int     | `1`                                              |
| `default.replication.factor`                              | Facteur de réplication par défaut par topic, utilisé dans les mêmes cas que `num.partitions` (création auto, topics internes Streams, `AdminClient#createTopics`).                                                                                                                                                                              | int     | `1`                                              |
| `min.insync.replicas`                                     | Nombre minimal de réplicas synchronisés (ISR), leader inclus, requis pour qu'une écriture réussisse quand un producer utilise `acks=all`. Si l'ISR contient moins de membres que cette valeur, le producer reçoit une exception.                                                                                                                | int     | `1`                                              |
| `log.retention.hours`                                     | Nombre d'heures de conservation d'un fichier de log avant suppression ; paramètre tertiaire par rapport à `log.retention.ms`.                                                                                                                                                                                                                   | int     | `168`                                            |
| `log.segment.bytes`                                       | Taille maximale d'un seul fichier de segment de log.                                                                                                                                                                                                                                                                                            | int     | `1073741824` (1 GiB)                             |
| `log.retention.check.interval.ms`                         | Fréquence (en ms) à laquelle le nettoyeur de logs vérifie si des logs sont éligibles à la suppression.                                                                                                                                                                                                                                          | long    | `300000` (5 min)                                 |
| `zookeeper.connect` *(supprimé depuis 4.0)*               | Chaîne de connexion au cluster ZooKeeper, au format `host:port`, avec possibilité de lister plusieurs hôtes (`host1:port1,host2:port2,...`) et d'ajouter un chemin *chroot* (`/chemin`) pour isoler les données du cluster dans le namespace ZooKeeper.                                                                                         | list    | —                                                |
| `zookeeper.connection.timeout.ms` *(supprimé depuis 4.0)* | Délai maximal (en ms) pour que le client établisse une connexion à ZooKeeper. Si non défini, reprend la valeur de `zookeeper.session.timeout.ms` (18000 ms).                                                                                                                                                                                    | int     | —                                                |
| `auto.create.topics.enable`                               | Active la création automatique de topics côté serveur.                                                                                                                                                                                                                                                                                          | boolean | `true`                                           |
| `broker.id`                                               | Identifiant du broker pour ce serveur.                                                                                                                                                                                                                                                                                                          | int     | `-1`                                             |
| `advertised.listeners`                                    | Adresses des listeners que les brokers annoncent aux clients et aux autres brokers — utile quand `listeners` ne représente pas les adresses réellement joignables par les clients (ex. environnements cloud/NAT). Si absent, la valeur de `listeners` est utilisée. Contrairement à `listeners`, ne peut pas annoncer l'adresse méta `0.0.0.0`. | list    | `null`                                           |
| `delete.topic.enable`                                     | Quand `true`, les topics peuvent être supprimés via l'admin client ; quand `false`, les requêtes de suppression sont explicitement rejetées par le broker.                                                                                                                                                                                      | boolean | `true`                                           |

</details>

## TD

<details>
<summary>Travaux Dirigés</summary>

# OrderFlow — squelette sans Kafka

Cas d'école événementiel. **Toute la logique métier est écrite et testée ; la
couche Kafka est à toi.** Voir [`TODO-KAFKA.md`](TODO-KAFKA.md).

- **Java 21** · **Spring Boot 4.1.1** · **H2 embarqué** · **Maven**
- Aucune dépendance à Docker, à un broker ou à un serveur de base de données
- Tout tourne depuis le répertoire utilisateur, sans droits administrateur

---

## Démarrer

### Prérequis : un JDK 21

Archive `.zip` (Windows) ou `.tar.gz` (Linux, macOS) d'Eclipse Temurin 21,
décompressée dans ton répertoire utilisateur. Pas d'installeur.

```bash
java -version   # doit afficher 21.x
```

### Prérequis : Maven

Le projet est livré **sans Maven Wrapper**, pour ne pas embarquer un script
que je n'ai pas pu tester. Deux options, toutes deux sans droits admin :

1. **Maven portable** — décompresser `apache-maven-3.9.16-bin.zip` et ajouter
   son `bin` au `PATH` utilisateur.
2. **Générer le wrapper** une fois Maven disponible, puis n'utiliser que lui :
   ```bash
   mvn -N wrapper:wrapper -Dmaven=3.9.16
   ```
   Tu obtiens `mvnw` / `mvnw.cmd`, et Maven n'a plus besoin d'être installé.

> Si ton réseau d'entreprise filtre Maven Central, configure le proxy dans
> `~/.m2/settings.xml` avant la première build. C'est le blocage le plus
> fréquent sur poste bridé.

### Compiler et tester

```bash
mvn clean verify
```

Aucun service externe n'est nécessaire : les tests sont des tests unitaires purs,
sans contexte Spring, sans base, sans broker.

### Lancer

Quatre terminaux, ou lance seulement ce dont tu as besoin :

```bash
mvn -pl order-service        spring-boot:run   # :8081
mvn -pl inventory-service    spring-boot:run   # :8082
mvn -pl payment-service      spring-boot:run   # :8083
mvn -pl notification-service spring-boot:run   # :8084
```

### Essayer

```bash
curl -X POST http://localhost:8081/api/orders -H "Content-Type: application/json" -d "{"customerId":"cust-118","items":[{"productId":"sku-001","quantity":2}]}"
```

Dans la console d'`order-service`, tu verras la ligne d'outbox partir :

```
[NO-BROKER] topic=orders.created key=ord-3f2a1b8c headers={eventId=..., eventType=OrderCreated, ...} payload={...}
```

C'est exactement le message que Kafka transportera : topic, clé de partition,
en-têtes, payload. Il ne va nulle part aujourd'hui — c'est ce que tu vas
brancher, en écrivant un `EventPublisher` et en basculant la propriété
`orderflow.messaging.publisher` sur `kafka`.

Voir aussi les requêtes prêtes à l'emploi dans [`http/`](http/).

---

## Architecture

```
orderflow-common/          Contrats partagés : événements, topics, port EventPublisher
order-service/       :8081 Source de vérité de la commande + outbox transactionnel
inventory-service/   :8082 Réservation, rejet, compensation du stock
payment-service/     :8083 Encaissement simulé (déterministe)
notification-service/:8084 Notification client (sans état)
```

Chaque service a **sa propre base H2**, dans `~/orderflow-data/`. Aucune base
partagée : c'est la règle qui rend l'architecture événementielle nécessaire
plutôt que décorative.

### Les deux scénarios à connaître

| Commande                  | Résultat    | Ce que ça exerce                                    |
|---------------------------|-------------|-----------------------------------------------------|
| `sku-001` × 2 → 39,80 €   | `CONFIRMED` | Parcours nominal complet                            |
| `sku-005` × 1 → 1250,00 € | `CANCELLED` | Refus de paiement + **compensation du stock**       |
| `sku-005` × 5             | `CANCELLED` | Rejet stock (il n'y en a que 2) — sans compensation |

Le simulateur de paiement refuse au-delà de 1000 € — règle déterministe,
configurable via `orderflow.payment.refusal-threshold`.

---

## Consulter les données

Console H2 sur chaque service : `http://localhost:8081/h2-console`
(JDBC URL `jdbc:h2:file:~/orderflow-data/orders`, utilisateur `sa`, pas de mot de passe).

Tables intéressantes :

- `orders`, `order_items`, **`outbox_event`** (côté order)
- `stock`, **`processed_events`** (côté inventory)
- `payments`, `processed_events` (côté payment)

Le contenu de `outbox_event` et `processed_events` est le meilleur support pour
comprendre les patterns avant même d'avoir branché Kafka.

---

## Points de vigilance Spring Boot 4

Le projet cible Boot 4.1.1, qui introduit deux ruptures par rapport à Boot 3 :

**Starters modulaires.** `spring-boot-starter-web` est devenu
`spring-boot-starter-webmvc`, `spring-boot-starter-json` est devenu
`spring-boot-starter-jackson`, et `spring-boot-starter-test` a été éclaté en
starters par technologie. Les anciens noms existent encore mais sont dépréciés.
Le `pom.xml` parent documente un filet de sécurité (`spring-boot-starter-classic`)
si un starter modulaire posait problème.

**Jackson 3.** Le bean auto-configuré n'est plus
`com.fasterxml.jackson.databind.ObjectMapper` mais
`tools.jackson.databind.json.JsonMapper`, immuable et thread-safe. Les exceptions
sont non checkées, et les types `java.time` sont sérialisés en ISO-8601
nativement, sans module à enregistrer. Les annotations, elles, restent dans
`com.fasterxml.jackson.annotation`.

**Côté tests**, si tu ajoutes des tranches (`@WebMvcTest`, `@DataJpaTest`), sache
que `@MockBean` a disparu au profit de `@MockitoBean`, que `@SpringBootTest` ne
configure plus MockMvc automatiquement, et que `@WebMvcTest` demande désormais le
starter `spring-boot-starter-webmvc-test`. Les tests livrés évitent
volontairement ces API : ce sont des tests unitaires purs, JUnit 5 + Mockito.

---

## Honnêteté sur ce livrable

Je n'ai **pas pu compiler ce projet** : mon environnement n'a pas accès à Maven
Central. Le code est écrit avec soin et les versions sont vérifiées, mais la
zone la plus susceptible de demander un ajustement est le nom exact de certains
starters Boot 4 dans les `pom.xml`. Si un starter ne résout pas, le bloc
`spring-boot-starter-classic` documenté dans le pom parent te débloque en une
minute.

La logique métier, elle, est du Java standard et ne dépend d'aucune de ces
subtilités.

</details>

<details>
<summary>Travaux Dirigés - Réalisation</summary>

* Ajouter les dépendances nécessaires pour implementer Kafka Spring `spring-kafka` dans `pom.xml` ainsi que l'ajout les
  paramètres dans `application.yml`

```xml

<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-kafka</artifactId>
</dependency>
<dependency>
<groupId>org.springframework.kafka</groupId>
<artifactId>spring-kafka-test</artifactId>
<scope>test</scope>
</dependency>
```

```yml 
spring:
  kafka:
    bootstrap-servers: localhost:9092
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer
      value-serializer: org.apache.kafka.common.serialization.StringSerializer
      acks: all
      properties:
        enable.idempotence: true
    consumer:
      group-id: order-service
      auto-offset-reset: earliest
      enable-auto-commit: false
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer
    listener:
      ack-mode: record
```

* Ajout des `Publisher`
  Kafka [KafkaEventPublisher.java](order-service/src/main/java/fr/orderflow/order/messaging/KafkaEventPublisher.java) en
  lieu et place du
  LoggingEventPublisher [LoggingEventPublisher.java](orderflow-common/src/main/java/fr/orderflow/common/messaging/LoggingEventPublisher.java)
  doit :
    * être un `@Component` pour que Spring le détecte comme un bean et le construit au runtime.
    * implémente `EventPublisher`, qui est déja prévu pour surcharger une méthode retournant topic key headers
      payload, qui seront nécessaires au `Publisher` Kafka.
    * définir la variable de
      type (`message à envoyé`).
      [ProducerRecord](https://kafka.apache.org/43/javadoc/org/apache/kafka/clients/producer/ProducerRecord.html) qui
      permet de créer des enregistrements.
    * ajouter à la variable de type ProducerRecord (`message à envoyé`) la liste contenu du header, au besoin.
    * envoi le `message` dans un `topic`.
    * La valeur du paramètre `orderflow.messaging.publisher` doit être `kafka` (vs logging initialement).
    * Exemple de `Publisher` :

```java

@Component
public class KafkaEventPublisher implements EventPublisher {

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaEventPublisher(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }


    @Override
    public void publish(EventEnvelope envelope) {

        ProducerRecord<String, String> stringStringProducerRecord = new ProducerRecord<>(
                envelope.topic(),
                envelope.key(),
                envelope.payload());

        envelope.headers()
                .forEach((stringKey, stringValue) -> stringStringProducerRecord.headers()
                        .add(stringKey, stringValue.getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(stringStringProducerRecord);

    }
}
```

* Ajout des `Listens` nécessaires pour la deserialisation et delegation à `KafkaListener`. _Chaque méthode (onReserved,
  onReject) gère un événement spécifique d'un topic distinct._. Les méthodes doivent :
    * être un `@Component`. La classe doit être enregistrée comme un bean Spring pour que le conteneur puisse détecter
      automatiquement les méthodes annotées pour l'écoute.
    * avoir des méthodes annotées `@KafkaListener` transforme une méthode en consommateur Kafka. Elle prend
      généralement en paramètre :
        * `topics` : Le ou les sujets Kafka écoutés.
        * `groupId` : L'identifiant du groupe de consommateurs (essentiel pour la répartition des charges et le suivi
          des offsets).
    * L'extraction du payload et des métadonnées (`@Payload` et `@Header`) : Permet de désosser le message entrant.
      `@Payload`
      récupère le corps brut du message (souvent du JSON), tandis que `@Header` extrait les métadonnées cruciales (comme
      le `correlationId` pour le traçage distribué ou l'`ID` de l'événement).
    * La désérialisation du message : Convertit la chaîne brute (ou les octets) du payload en un objet typé Java (ex:
      `InventoryReservedEvent`) via un composant dédié (comme votre `EventSerializer`).
    * La délégation au service métier (Service Layer) : Le listener ne doit pas contenir de logique métier. Son rôle se
      limite à recevoir, décoder et transmettre. Il délègue immédiatement le traitement à un service (ex:
      `OrderService`)
      en lui passant les données extraites. _L'injection des dépendances (OrderService, EventSerializer) se fait via le
      constructeur._
* Exemple de `Listens` :

```java

@Component
public class InventoryEventListen {

    private final OrderService orderService;

    private final EventSerializer eventSerializer;

    public InventoryEventListen(OrderService orderService, EventSerializer eventSerializer) {
        this.orderService = orderService;
        this.eventSerializer = eventSerializer;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "order-service")
    public void onReserved(@Payload String payload, @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        var event = eventSerializer.fromJson(payload, InventoryReservedEvent.class);
        orderService.onInventoryReserved(event.orderId(), correlationId);
    }

    @KafkaListener(topics = Topics.INVENTORY_REJECTED, groupId = "order-service")
    public void onReject(@Payload String payload, @Header(EventHeaders.CORRELATION_ID) String correlationId) {
        var event = eventSerializer.fromJson(payload, InventoryRejectedEvent.class);
        orderService.onInventoryRejected(event.orderId(), event.reason(), correlationId);
    }

}
```

* Ajout des `Topics` :
    * Les noms de Topics (String) doivent être déclarés dans un fichier de constantes. Dans notre cas, dans le module
      common, exemple de déclaration :

```java
public final class Topics {

    public static final String ORDERS_CREATED = "orders.created";
    public static final String ORDERS_CANCELLED = "orders.cancelled";
    public static final String ORDERS_CONFIRMED = "orders.confirmed";
    public static final String INVENTORY_RESERVED = "inventory.reserved";
    public static final String INVENTORY_REJECTED = "inventory.rejected";
    public static final String PAYMENTS_COMPLETED = "payments.completed";
    public static final String PAYMENTS_FAILED = "payments.failed";

    private Topics() {
    }
}
```

*
    *

</details>