# src/metrics.py
import threading
import time
from collections import defaultdict, deque

from .broker import Broker
from .offsets import OffsetStore


class Metrics:
    """
    Métriques d'observabilité pour le pipeline distribué.

    - backlog   : nombre de messages non encore traités par partition
    - lag total : somme des backlogs sur toutes les partitions
    - throughput: événements traités par seconde par consumer (fenêtre glissante)
    """

    def __init__(self, broker: Broker, offsets: OffsetStore, group: str, window_s: int = 5):
        self.broker = broker
        self.offsets = offsets
        self.group = group
        self.window = window_s
        self._history: dict[str, deque] = defaultdict(deque)  # consumer_id -> timestamps
        self._lock = threading.Lock()

    def mark_processed(self, consumer: str, partition: int) -> None:
        """Enregistre un événement traité par un consumer (pour calcul throughput)."""
        now = time.time()
        with self._lock:
            dq = self._history[consumer]
            dq.append(now)
            # Purger les timestamps hors de la fenêtre glissante
            while dq and now - dq[0] > self.window:
                dq.popleft()

    def snapshot(self) -> dict:
        """Retourne un instantané des métriques clés."""
        backlog = {}
        total_lag = 0
        for p in range(self.broker.num_partitions):
            sz = self.broker.size(p)
            off = self.offsets.get(self.group, p)
            backlog[p] = max(0, sz - off)
            total_lag += backlog[p]

        throughput = {}
        with self._lock:
            for c, dq in self._history.items():
                throughput[c] = round(len(dq) / self.window, 2)

        return {
            "backlog_by_partition": backlog,
            "total_lag": total_lag,
            "throughput_per_consumer": throughput,
        }