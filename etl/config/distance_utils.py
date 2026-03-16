"""
Utilitaires pour le calcul des distances geographiques.
"""

import math
from typing import Tuple


def haversine(
    lat1: float, lon1: float,
    lat2: float, lon2: float
) -> float:
    """
    Calcule la distance entre deux points GPS avec la formule de Haversine.

    Args:
        lat1, lon1: Coordonnees du point de depart (degres)
        lat2, lon2: Coordonnees du point d'arrivee (degres)

    Returns:
        Distance en kilometres
    """
    R = 6371  # Rayon moyen de la Terre en km

    # Convertir en radians
    lat1_r, lon1_r, lat2_r, lon2_r = map(math.radians, [lat1, lon1, lat2, lon2])

    # Differences
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r

    # Formule de Haversine
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_r) * math.cos(lat2_r) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))

    return R * c


def haversine_batch(
    coords: Tuple[float, float, float, float]
) -> float:
    """
    Version vectorisable pour apply() sur un DataFrame.
    Prend un tuple (lat1, lon1, lat2, lon2).
    """
    return haversine(*coords)


# Pour utilisation SQL directe si besoin
def get_haversine_sql(column_lat1: str, column_lon1: str,
                       column_lat2: str, column_lon2: str) -> str:
    """
    Retourne l'expression SQL pour calculer Haversine en PostgreSQL.
    Utile pour faire le calcul directement en base.
    """
    return f"""
        (6371 * acos(
            cos(radians({column_lat1})) * cos(radians({column_lat2})) *
            cos(radians({column_lon2}) - radians({column_lon1})) +
            sin(radians({column_lat1})) * sin(radians({column_lat2}))
        ))::numeric(10,2)
    """
