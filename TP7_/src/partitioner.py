# src/partitioner.py
import hashlib


def partition_of(key: str, num_partitions: int) -> int:
    """
    Détermine le numéro de partition d'une clé par hachage SHA-256.
    Garantit une distribution uniforme et déterministe : une même clé
    arrive toujours dans la même partition.
    """
    h = hashlib.sha256(key.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big") % num_partitions


if __name__ == "__main__":
    from collections import Counter

    # Distribution uniforme
    keys_uniforme = [f"sensor-{i:03d}" for i in range(200)]
    repartition = Counter(partition_of(k, 4) for k in keys_uniforme)
    print("Uniforme :", repartition)

    # Distribution skew (hotspot)
    keys_skew = ["sensor-HOT-01"] * 180 + [f"sensor-{i}" for i in range(20)]
    repartition_skew = Counter(partition_of(k, 4) for k in keys_skew)
    print("Skew     :", repartition_skew)