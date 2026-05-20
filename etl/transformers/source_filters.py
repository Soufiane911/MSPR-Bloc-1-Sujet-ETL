"""Filtres metier par source pour limiter le bruit regional dans l'ETL."""

from __future__ import annotations

import pandas as pd
from config.logging_config import setup_logging

# Operateurs pertinents dans le flux CFF/SBB (SBB + liaisons SNCF en Suisse)
CFF_SBB_LONG_DISTANCE_AGENCIES = {"11", "87_LEX"}

# Prefixes de lignes grande distance / internationales (SBB et correspondances)
CFF_SBB_LONG_DISTANCE_PREFIXES = (
    "IC",
    "EC",
    "IR",
    "EXT",
    "EN",
    "NJ",
    "RJX",
    "ICE",
    "TGV",
)


def _route_short_name_is_long_distance(short_name: object) -> bool:
    if pd.isna(short_name):
        return False
    name = str(short_name).strip().upper()
    return any(name.startswith(prefix) for prefix in CFF_SBB_LONG_DISTANCE_PREFIXES)


def filter_cff_sbb_long_distance(routes_df: pd.DataFrame) -> pd.DataFrame:
    """
    Ne conserve que les lignes SBB/SNCF grande distance du flux CFF/SBB.

    Le GTFS suisse agrege de nombreux operateurs regionaux (BLS, transports
    urbains, etc.) ; ce filtre aligne le perimetre sur les corridors internationaux.
    """
    if routes_df.empty:
        return routes_df

    logger = setup_logging("transformer.filter.cff_sbb")
    initial_count = len(routes_df)
    filtered = routes_df.copy()

    if "agency_id" in filtered.columns:
        filtered["agency_id"] = filtered["agency_id"].astype(str)
        filtered = filtered[
            filtered["agency_id"].isin(CFF_SBB_LONG_DISTANCE_AGENCIES)
        ]

    if "route_short_name" in filtered.columns:
        mask = filtered["route_short_name"].map(_route_short_name_is_long_distance)
        filtered = filtered[mask].copy()

    removed = initial_count - len(filtered)
    if removed > 0:
        logger.info(
            f"[FILTER] cff_sbb: {removed}/{initial_count} routes supprimees "
            f"(hors SBB/SNCF grande distance)"
        )

    return filtered
