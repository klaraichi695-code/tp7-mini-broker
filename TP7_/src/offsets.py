# src/offsets.py
import json
import threading
from pathlib import Path


class OffsetStore:
    """
    Gestion des offsets de lecture par groupe et par partition.

    - get()        : lecture de l'offset courant (mémoire)
    - set_memory() : mise à jour en mémoire seulement (entre deux commits)
    - commit()     : écriture atomique sur disque (reprise après crash)
    - flush()      : force le commit de l'offset mémoire courant
    """

    def __init__(self, path: str = "state/offsets.json"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                return {}
        return {}

    def get(self, group: str, partition: int) -> int:
        """Retourne l'offset courant (0 si jamais commité)."""
        return self._data.get(group, {}).get(str(partition), 0)

    def set_memory(self, group: str, partition: int, offset: int) -> None:
        """Met à jour l'offset en mémoire sans écrire sur disque."""
        with self._lock:
            self._data.setdefault(group, {})[str(partition)] = offset

    def commit(self, group: str, partition: int, offset: int) -> None:
        """Persiste l'offset sur disque via écriture atomique (write + rename)."""
        with self._lock:
            self._data.setdefault(group, {})[str(partition)] = offset
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self._data, indent=2), encoding="utf-8")
            tmp.replace(self.path)

    def flush(self, group: str, partition: int) -> None:
        """Force la persistance de l'offset mémoire courant."""
        off = self.get(group, partition)
        self.commit(group, partition, off)

    def all_offsets(self) -> dict:
        return dict(self._data)