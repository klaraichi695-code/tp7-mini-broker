# src/storage.py
import json
import threading
from pathlib import Path

from .events import Event


class Storage:
    """
    Persistance partitionnée sur disque au format JSONL.

    Arborescence produite :
        outputs/
          date=YYYY-MM-DD/
            site=SITE-XX/
              partition=N/
                events.jsonl

    Cette structure reproduit le partitionnement Hive/Spark habituel
    dans un data lake (format Parquet ou JSONL).
    """

    def __init__(self, base: str = "outputs"):
        self.base = Path(base)
        self.base.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def write(self, evt: Event, partition: int) -> Path:
        """Écrit un événement dans le fichier JSONL correspondant à sa partition."""
        date = evt.event_time[:10]   # YYYY-MM-DD
        site = evt.site_id
        target_dir = self.base / f"date={date}" / f"site={site}" / f"partition={partition}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target_file = target_dir / "events.jsonl"
        line = json.dumps(evt.to_dict(), ensure_ascii=False)
        with self._lock:
            with target_file.open("a", encoding="utf-8") as f:
                f.write(line + "\n")
        return target_file