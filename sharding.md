

# La gestion mémoire ES

Elastic Search utilise du sharding pour stocker son index :
Avantage : permet d'avoir un index volumineux, d'augmenter la taille de l'index (scalable). Répartir les shards sur différents nodes permet aussi de paralléliser les algorithmes sur l'index (système distribué)

## Naviguer dans le cluster ES

Tout d'abord relançons le cluster sur 1 container :

```bash
docker compose up
```

Vision globale des nodes et shards :
```bash
curl -u elastic:111 "http://localhost:9200/_cluster/health?pretty"
```

Visualiser tous les shards :
```bash
curl -u elastic:111 "http://localhost:9200/_cat/shards?v"
#  (?v permet d'afficher le nom des colonnes)
```

Comprendre les infos :
```
index                                                              shard prirep state        docs   store dataset ip         node
ncedc-earthquakes-earthquake                                       0     p      STARTED    779095 154.1mb 154.1mb 172.18.0.2 9407c7cd63a6
```

Si plusieurs containers ES sont configurés et lancés, alors les shards sont répartis sur plusieurs nodes

Inspecter les shards pertinents : (Cette commande utile sera réutilisée plus tard pour consulter l'état du cluster)
```
curl -su elastic:111 "http://localhost:9200/_cat/shards?v&h=index,shard,prirep,state,store&s=store:desc" | grep -v "^\."
```
## Lancer plusieurs nodes

Un data node contient des shards avec les documents indexés. 

Les documents sont stockés sur des data shards, et ils peuvent être redondants. Il y toujours un primary shard et 0 ou plus répliques, ce qui permet de tolérer et réparer les erreurs, mais aussi de répartir en parallèle les opérations d'écriture/lecture sur un même segment de donnée. Si un shard primaire est corrumpu, une réplique est promue en primaire à sa place.

### Démarrer les nodes

La suite de ce tutoriel consiste à relancer 3 containers qui contiennent chacun 1 node ES :

```bash
docker compose down
docker compose -f docker-compose-multi.yml up -d --build
```
En inspectant les shards, les index doivent bien avoir été répartis sur des nodes différents.

### Configurer des répliques

Il est possible de configurer la réplication des shards, ici on configure 2 répliques pour tous les shards du cluster :

```bash
curl -u elastic:111 -X PUT "http://localhost:9200/_settings" \
-H 'Content-Type: application/json' \
-d @elastic-settings/add-replicas.json
```
On peut maintenant voir les modifications dans le cluster avec les commandes de la partie précédente.

### Dupliquer les nodes primaires :

Le nombre de shards (redondance de l'information) n'est pas modifiable, il faut copier l'index dans un nouveau avec un nombre de shards supérieur (splitting). Dans ce cas nous allons l'effectuer seulement sur l'index `ncedc-earthquakes-earthquake` :

```bash
# Changer les droits d'écriture de l'index
curl -u elastic:111 -X PUT "http://localhost:9200/ncedc-earthquakes-earthquake/_settings" \
  -H "Content-Type: application/json" \
  -d '{"index.blocks.write": true}'

# Splitter vers un nouvel index
curl -u elastic:111 -X POST "http://localhost:9200/ncedc-earthquakes-earthquake/_split/ncedc-earthquakes-earthquake-v2" \
  -H "Content-Type: application/json" \
  -d '{"settings": {"index.number_of_shards": 3}}'
```
Toujours en inspectant le cluster on peut voir la duplication de la donnée et l'augmentation de la taille effective de l'index


## Un intérêt d'efficacité

L'idée est de répartir la charge CPU des algorithmes sur plusieurs data node afin de paralélliser et accélérer le processus de recherche. Ces nodes peuvent être de type plus spécifique en fonction du type et de la fréquence d'accès : data, data_content, data_hot, data_warm, data_cold, data_frozen, ingest

![Data processing flow](img/elasticsearch-data-processing.png)

Un des nodes est le master, et a pour role d'orchestrer les opérations importantes dans l'index : ajout, modification ou suppression dans l'index, répertorier les autres nodes, répartir les shards dans les autres nodes
Cette répartition dynamique des rôles permet encore plus de scalabilité, de résilience et d'efficacité.

![Master node and roles](img/master-node.png)

Pour plus d'information, la documentation ES est très détaillée et accessible
