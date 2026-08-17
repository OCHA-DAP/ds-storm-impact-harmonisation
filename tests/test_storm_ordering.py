"""Tests for storm-card ordering in the monitor email.

Storms are ordered by total people exposed, largest first, so the reader
sees the headline storm without scrolling. The sort reorders whole storms
only — each storm's per-country blocks stay together and keep their own
ordering (countries by exposure, wind bands by severity), which is handled
in `_storm_section_html` and untouched here.
"""

import pandas as pd

from src.gdacs_monitor_email import sort_storms_by_exposure


def _active(*names):
    return pd.DataFrame(
        {"eventid": range(1, len(names) + 1), "name": list(names)}
    )


def _exp(rows):
    return pd.DataFrame(rows, columns=["eventid", "iso3", "buffer", "pop_affected"])


class TestSortStormsByExposure:
    def test_largest_total_exposure_first(self):
        active = _active("SMALL", "BIG")
        exposure = _exp([
            (1, "PHL", "buffer39", 1_000),
            (2, "PHL", "buffer39", 500_000),
        ])
        out = sort_storms_by_exposure(active, exposure)
        assert out["name"].tolist() == ["BIG", "SMALL"]

    def test_storm_total_sums_countries_at_their_max_threshold(self):
        # Per country, the figure is its max across wind thresholds (the
        # same figure the card's country ordering uses) — NOT the sum of
        # every threshold row, which would double-count nested footprints.
        # Storm 1: one country, rows 100k + 90k (max 100k).
        # Storm 2: two countries at 60k each (total 120k) — ranks first,
        # though a naive sum of all rows would put storm 1 (190k) first.
        active = _active("NESTED", "TWO-COUNTRY")
        exposure = _exp([
            (1, "PHL", "buffer39", 100_000),
            (1, "PHL", "buffer64", 90_000),
            (2, "VNM", "buffer39", 60_000),
            (2, "KHM", "buffer39", 60_000),
        ])
        out = sort_storms_by_exposure(active, exposure)
        assert out["name"].tolist() == ["TWO-COUNTRY", "NESTED"]

    def test_storm_without_exposure_rows_sorts_last(self):
        # An open-ocean storm has no exposure rows: zero people exposed is
        # its real figure, so it belongs at the bottom, not dropped.
        active = _active("OCEAN", "LANDFALL")
        exposure = _exp([(2, "FJI", "buffer39", 10_000)])
        out = sort_storms_by_exposure(active, exposure)
        assert out["name"].tolist() == ["LANDFALL", "OCEAN"]
        assert len(out) == 2

    def test_ties_keep_gdacs_order(self):
        active = _active("FIRST", "SECOND")
        exposure = _exp([
            (1, "PHL", "buffer39", 5_000),
            (2, "VNM", "buffer39", 5_000),
        ])
        out = sort_storms_by_exposure(active, exposure)
        assert out["name"].tolist() == ["FIRST", "SECOND"]

    def test_empty_exposure_returns_active_unchanged(self):
        active = _active("A", "B")
        out = sort_storms_by_exposure(active, _exp([]))
        assert out["name"].tolist() == ["A", "B"]

    def test_index_is_reset(self):
        # Downstream code iterates positionally; a leftover shuffled index
        # would silently misalign anything that uses .loc/.iloc mixups.
        active = _active("SMALL", "BIG")
        exposure = _exp([
            (1, "PHL", "buffer39", 1),
            (2, "PHL", "buffer39", 2),
        ])
        out = sort_storms_by_exposure(active, exposure)
        assert out.index.tolist() == [0, 1]
