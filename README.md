# tp7-mini-broker

## Objectif

Simuler un broker Kafka-like avec partitionnement par clé, consumer group, offsets persistants et garantie at-least-once.

---

## Prérequis

- Python 3.11+

---

## Installation

```bash
cd TPs/TP7_Mini_Broker
# Pas de dépendances externes
Exécution
bash
python run.py
# ou avec options
python run.py --partitions 4 --consumers 2 --duration 30
Structure
text
TP7_Mini_Broker/
├── run.py                   # Point d'entrée
├── src/
│   ├── main.py              # Orchestration
│   ├── events.py            # Dataclass Event
│   ├── broker.py            # MiniBroker (N partitions)
│   ├── partitioner.py       # SHA-256 → partition
│   ├── consumers.py         # Consumer + assignation
│   ├── offsets.py           # OffsetStore (persistant)
│   ├── metrics.py           # Backlog, lag, throughput
│   └── storage.py           # Stockage JSONL
├── outputs/                 # Données produites
├── logs/                    # Journaux
└── state/                   # Offsets persistants
Résultats
Indicateur	Valeur
Partitions	4
Consumers	2
Messages produits	~450
Backlog final	0
Fichiers générés :

outputs/date=.../site=.../partition=N/events.jsonl

state/offsets.json

logs/run.log

Assignation des partitions
Consumer	Partitions
C0	[0, 2]
C1	[1, 3]
Règle : une partition ne peut être lue que par un seul consumer par groupe.

Sortie console
text
=== Démarrage pipeline Séance 7 ===
Partitions=4 | Consumers=2 | key_field=sensor_id | groupe=agri-stats
Assignation : {'C0': [0, 2], 'C1': [1, 3]}

[DASHBOARD] backlog=  2 | success= 45 | fail=  5 | retry=  8 | rate= 12.3 msg/s
[DASHBOARD] backlog=  0 | success=150 | fail= 15 | retry= 25 | rate= 15.8 msg/s

Offsets finaux : {'agri-stats': {'0': 84, '1': 0, '2': 169, '3': 353}}
Partitions broker : {0: 84, 1: 0, 2: 169, 3: 353}
=== Pipeline arrêté ===
Garantie at-least-once
L'offset est commité toutes les 5 lectures. En cas de crash entre deux commits, les derniers messages seront relus (traitement idempotent requis).

Difficultés rencontrées
Partitionnement : choisir la bonne clé (city vs sensor_id vs order_id)

Skew : si une clé est sur-représentée, sa partition est surchargée

Écriture atomique : fichier .tmp + rename() pour les offsets
