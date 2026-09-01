# Kafka

## Définitions & concepts

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
    * Permattant de lire et de relire les messages
    * Les messages ne sont pas détruits
* `Commit log`
    * Méthode empoyé par Kafka pour stocker les messages
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

* Configuration cluster et noeuf Kafka. Ouvrir pour modifier/ contrôler la configuration kafka dans le fichier `config/server.properties` ou (et) `controller.properties` ou (et) `broker.properties` (Exemple : [server.properties](server.properties))):

| Paramètre | Définition | Type | Défaut |
|---|---|---|---|
| `log.dirs` | Liste de répertoires (séparés par des virgules) où sont stockées les données de log. Si absent, la valeur de `log.dir` est utilisée. | list | `null` (repli sur `log.dir` = `/tmp/kafka-logs`) |
| `num.partitions` | Nombre par défaut de partitions par topic. S'applique à la création automatique de topics, à la création de topics internes de Kafka Streams, et à `AdminClient#createTopics` quand le nombre de partitions vaut -1. | int | `1` |
| `default.replication.factor` | Facteur de réplication par défaut par topic, utilisé dans les mêmes cas que `num.partitions` (création auto, topics internes Streams, `AdminClient#createTopics`). | int | `1` |
| `min.insync.replicas` | Nombre minimal de réplicas synchronisés (ISR), leader inclus, requis pour qu'une écriture réussisse quand un producer utilise `acks=all`. Si l'ISR contient moins de membres que cette valeur, le producer reçoit une exception. | int | `1` |
| `log.retention.hours` | Nombre d'heures de conservation d'un fichier de log avant suppression ; paramètre tertiaire par rapport à `log.retention.ms`. | int | `168` |
| `log.segment.bytes` | Taille maximale d'un seul fichier de segment de log. | int | `1073741824` (1 GiB) |
| `log.retention.check.interval.ms` | Fréquence (en ms) à laquelle le nettoyeur de logs vérifie si des logs sont éligibles à la suppression. | long | `300000` (5 min) |
| `zookeeper.connect` *(supprimé depuis 4.0)* | Chaîne de connexion au cluster ZooKeeper, au format `host:port`, avec possibilité de lister plusieurs hôtes (`host1:port1,host2:port2,...`) et d'ajouter un chemin *chroot* (`/chemin`) pour isoler les données du cluster dans le namespace ZooKeeper. | list | — |
| `zookeeper.connection.timeout.ms` *(supprimé depuis 4.0)* | Délai maximal (en ms) pour que le client établisse une connexion à ZooKeeper. Si non défini, reprend la valeur de `zookeeper.session.timeout.ms` (18000 ms). | int | — |
| `auto.create.topics.enable` | Active la création automatique de topics côté serveur. | boolean | `true` |
| `broker.id` | Identifiant du broker pour ce serveur. | int | `-1` |
| `advertised.listeners` | Adresses des listeners que les brokers annoncent aux clients et aux autres brokers — utile quand `listeners` ne représente pas les adresses réellement joignables par les clients (ex. environnements cloud/NAT). Si absent, la valeur de `listeners` est utilisée. Contrairement à `listeners`, ne peut pas annoncer l'adresse méta `0.0.0.0`. | list | `null` |
| `delete.topic.enable` | Quand `true`, les topics peuvent être supprimés via l'admin client ; quand `false`, les requêtes de suppression sont explicitement rejetées par le broker. | boolean | `true` |