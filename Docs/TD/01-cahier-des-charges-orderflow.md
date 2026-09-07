# Cahier des charges — OrderFlow (v2)

**Plateforme événementielle de gestion de commandes e-commerce**
**Stack imposée : Java 21 · Spring Boot 4.1 · Spring Kafka 4.1 · Apache Kafka 4.3 (KRaft)**
**Contrainte majeure : poste de développement SANS droits administrateur — aucun Docker, aucun installeur**

---

## 1. Contexte et objectifs

Cas d'école destiné à un développeur Java/Spring expérimenté souhaitant s'entraîner en autonomie au développement d'une solution événementielle **Apache Kafka**.

Une plateforme e-commerce fictive veut rendre son traitement de commandes **asynchrone, résilient et découplé**, en remplaçant des appels synchrones inter-services par une architecture pilotée par les événements.

### Objectifs pédagogiques

- Concevoir producers/consumers Kafka avec Spring Kafka 4.x
- Modéliser un catalogue d'événements métier (topics, clés, schémas, partitionnement)
- Mettre en œuvre les patterns Kafka : **Outbox transactionnel**, **Idempotent Consumer**, **retry topics / Dead Letter Topic**, **Saga chorégraphiée**
- Faire évoluer un format d'événement JSON vers **Avro + registre de schémas**
- Mettre en place une **observabilité** de bout en bout (métriques, logs corrélés, tracing distribué)
- Écrire des **tests d'intégration** réalistes **sans Docker** (`@EmbeddedKafka` + H2)
- Exploiter **Java 21** (records, sealed interfaces, pattern matching, virtual threads) dans un contexte Kafka
- *(Bonus)* Introduire **Kafka Streams** pour un agrégat temps réel

---

## 2. Cas d'usage métier

**OrderFlow** traite les commandes de bout en bout :

1. Un client passe une commande (liste d'articles, quantités).
2. Le stock est vérifié et réservé.
3. Le paiement est déclenché.
4. Le client est notifié du résultat.
5. *(Bonus)* Une expédition est planifiée et un tableau de bord temps réel suit l'activité.

Si le stock est insuffisant **ou** si le paiement échoue, la commande est **automatiquement annulée** et les réservations compensées — sans transaction distribuée ni orchestrateur central : chaque service réagit aux événements des autres (**saga chorégraphiée**).

---

## 3. Périmètre fonctionnel

### Inclus
- Services : Order, Inventory, Payment, Notification
- API REST de déclenchement et de consultation
- Traitement asynchrone via Kafka entre tous les services
- Gestion des erreurs et des messages invalides (DLT)
- *(Bonus)* Service Shipping, service Analytics temps réel (Kafka Streams)

### Exclus (simplifiés ou simulés)
- Prestataire de paiement réel (un simulateur avec règle d'acceptation/refus suffit)
- Logistique de livraison réelle
- Authentification/autorisation complète (un `customerId` dans le payload suffit)
- Interface graphique riche (fichiers `.http` ou Postman suffisent)
- Déploiement production, conteneurisation, Kubernetes — **hors périmètre par construction** (voir §6)

---

## 4. Exigences fonctionnelles

| ID | Exigence | Priorité |
|----|----------|----------|
| EF-01 | Créer une commande via API REST (articles, quantités, identifiant client) | MUST |
| EF-02 | La création d'une commande publie un événement `OrderCreated` sur Kafka | MUST |
| EF-03 | Inventory consomme `OrderCreated`, réserve le stock, publie `InventoryReserved` ou `InventoryRejected` | MUST |
| EF-04 | Payment consomme la réservation validée, simule un paiement, publie `PaymentCompleted` ou `PaymentFailed` | MUST |
| EF-05 | En cas d'échec (stock ou paiement), la commande passe en `CANCELLED` et les réservations sont compensées | MUST |
| EF-06 | Notification consomme les événements terminaux et simule l'envoi d'une notification | MUST |
| EF-07 | Statut et historique d'une commande consultables via API GET | SHOULD |
| EF-08 | Tableau de bord agrégeant en temps réel commandes par statut et chiffre d'affaires | COULD |
| EF-09 | Shipping déclenche une expédition simulée après paiement confirmé | COULD |
| EF-10 | Vue d'administration pour consulter/rejouer les messages en erreur (DLT) | COULD |

---

## 5. Exigences non fonctionnelles

| ID | Exigence |
|----|----------|
| NF-01 | **Fiabilité** — aucun message métier perdu (livraison *at-least-once* garantie) |
| NF-02 | **Idempotence** — le retraitement d'un même événement ne duplique pas les effets métier |
| NF-03 | **Résilience** — arrêt/redémarrage d'un consumer sans perte ni doublon |
| NF-04 | **Traçabilité** — chaque flux suivable de bout en bout via un identifiant de corrélation |
| NF-05 | **Testabilité** — tests d'intégration sur les flux critiques, **exécutables sans Docker ni service externe** |
| NF-06 | **Performance indicative** — absorber ~100 commandes/seconde en local sans erreur applicative |
| NF-07 | **Évolutivité** — ajout d'un consommateur sans modifier les producteurs existants |
| NF-08 | **Documentation** — chaque service documente les topics qu'il consomme/produit |
| NF-09 | **Portabilité poste bridé** — toute l'infrastructure démarre depuis le répertoire utilisateur, sans élévation de privilèges, sans service Windows, sans port privilégié (< 1024) |

---

## 6. Contraintes techniques imposées

### 6.1 Contrainte « pas de droits administrateur »

Cette contrainte est **structurante** et non négociable. Elle implique :

| Interdit | Retenu à la place |
|---|---|
| Docker / Docker Desktop / Podman | Kafka lancé nativement depuis son archive `.tgz` |
| WSL2 (son installation demande l'admin) | Exécution native Windows via `bin\windows\*.bat` |
| Testcontainers (dépend de Docker) | `@EmbeddedKafka` (broker KRaft in-process) + H2 en mémoire |
| PostgreSQL installé en service | **H2 embarqué** (JAR Maven, fichier local, zéro installation) |
| Installeurs `.msi` / `.pkg` / `apt install` | Archives `.zip` / `.tar.gz` décompressées dans `%USERPROFILE%` ou `$HOME` |
| Modification du `PATH` système | Variables d'environnement **utilisateur** (`setx`) ou script de démarrage local |
| Ports < 1024 | Tous les ports en 8080–9099 |

### 6.2 Versions imposées (vérifiées au 7 septembre 2026)

| Composant | Version | Mode d'obtention sans admin |
|---|---|---|
| **JDK** | Eclipse Temurin **21.0.12.1+1** (LTS) | archive `.zip` (Windows) / `.tar.gz` (Linux, macOS) — **jamais** le `.msi` |
| **Apache Kafka** | **4.3.1** (build Scala 2.13) | `kafka_2.13-4.3.1.tgz`, décompression simple |
| **Spring Boot** | **4.1.1** | résolu par Maven, rien à installer |
| **Spring Framework** | 7.0.9 (embarqué par Boot 4.1.1) | via BOM |
| **Spring for Apache Kafka** | **4.1.1** (embarqué par Boot 4.1.1) | via BOM |
| **Maven** | **3.9.16** — ou, mieux, le **Maven Wrapper** (`mvnw`) | wrapper : rien à installer du tout |
| **H2 Database** | 2.4.240 (version gérée par le BOM Boot) | dépendance Maven |
| **AKHQ** (UI Kafka) | **0.28.0** | JAR exécutable `akhq-0.28.0-all.jar` |
| **Apicurio Registry** *(phase 7)* | 3.3.0 | JAR Quarkus `-runner.jar` (voir plan B au §13, phase 7 du dossier technique) |

Notes de compatibilité :
- Kafka 4.x fonctionne **uniquement en mode KRaft** (Zookeeper supprimé) et exige **Java 17+** — Java 21 convient.
- Spring Boot 4.1 exige Java 17 minimum et supporte jusqu'à Java 26 ; Java 21 LTS reste la cible du projet. Java 25 LTS serait une alternative valable pour pousser plus loin.
- **Piège Spring Boot 4** : le starter Kafka doit être déclaré **explicitement** (`spring-boot-starter-kafka`) ; il n'est plus tiré implicitement comme en Boot 3.

### 6.3 Autres contraintes
- Un microservice = une base de données dédiée (un **fichier H2** distinct par service) — pas de base partagée
- Build **Maven** multi-modules (recommandé) ou Gradle
- Tests d'intégration via `spring-kafka-test` (`@EmbeddedKafka`), **pas** Testcontainers

---

## 7. Livrables attendus

1. Dépôt de code source (mono-repo multi-modules recommandé)
2. **Scripts de démarrage locaux** (`start-kafka.cmd` / `start-kafka.sh`, `start-akhq.cmd` / `.sh`) remplaçant le `docker-compose.yml`
3. Documentation technique par service (topics consommés/produits, schémas d'événements, endpoints)
4. Suite de tests automatisés (unitaires + intégration `@EmbeddedKafka`), **exécutable sur un poste vierge sans droits admin**
5. Collection Postman ou fichiers `.http`
6. *(Bonus)* Captures ou dashboards de l'observabilité mise en place

---

## 8. Critères de réussite (Definition of Done global)

- [ ] Parcours nominal de bout en bout : commande → stock → paiement → notification
- [ ] Parcours de compensation : stock KO **ou** paiement KO → commande `CANCELLED`
- [ ] Un message invalide (« poison pill ») n'interrompt pas le consumer et finit en Dead Letter Topic
- [ ] Un redémarrage de consumer ne duplique pas les effets métier (idempotence vérifiée par test)
- [ ] `mvnw verify` passe de façon reproductible **sans qu'aucun service externe ne tourne**
- [ ] L'ensemble de la stack locale démarre depuis le répertoire utilisateur, sans élévation de privilèges

---

## 9. Feuille de route indicative

| Phase | Thème | Effort |
|-------|-------|--------|
| 0 | Socle local sans admin (JDK portable, Kafka KRaft, AKHQ) | 0,5 j |
| 1 | Premier flux Order → Inventory | 1 j |
| 2 | Ajout Payment + saga chorégraphiée | 1 j |
| 3 | Idempotence + Outbox transactionnel | 1 j |
| 4 | Résilience : retry topics + DLT | 0,5 j |
| 5 | Observabilité (métriques, tracing, logs corrélés) | 1 j |
| 6 | Notification + API de consultation | 0,5 j |
| 7 | Migration Avro (+ registre de schémas si accessible) | 1 j |
| 8 | *(Bonus)* Kafka Streams — dashboard temps réel | 1 j |
| 9 | *(Bonus)* Exactly-once, chaos testing, virtual threads | 1 j+ |

*Détail de chaque phase, critères de validation et procédures d'installation : dossier technique et fonctionnel.*
