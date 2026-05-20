"""Tests des filtres metier par source."""

import pandas as pd

from transformers.source_filters import filter_cff_sbb_long_distance


def test_filter_cff_sbb_keeps_long_distance_sbb_routes():
    routes = pd.DataFrame(
        {
            "route_id": ["r1", "r2", "r3", "r4"],
            "agency_id": ["11", "11", "33", "11"],
            "route_short_name": ["IC1", "S10", "IC5", "EC90"],
            "route_type": [102, 109, 102, 102],
        }
    )

    filtered = filter_cff_sbb_long_distance(routes)

    assert set(filtered["route_id"]) == {"r1", "r4"}


def test_filter_cff_sbb_keeps_sncf_cross_border_prefix():
    routes = pd.DataFrame(
        {
            "route_id": ["r1"],
            "agency_id": ["87_LEX"],
            "route_short_name": ["TGV Lyria"],
            "route_type": [102],
        }
    )

    filtered = filter_cff_sbb_long_distance(routes)

    assert len(filtered) == 1
