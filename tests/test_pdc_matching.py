"""Tests for GDACS→PDC storm matching (src/datasets/pdc.py).

Matching is by position + recency, not name: GDACS keeps a storm's
pre-naming designation ("ONE-C-26") for the event's whole lifetime, while
PDC adopts the agency-assigned name ("Tropical Storm Lala"), so name
matching produces false negatives exactly when a storm gets named.
Positions on both sides come from the same agency advisory, so proximity
is near-exact when the two feeds are in sync and off by at most one
6-hour advisory step (~200 km) when they are not.
"""

import pytest

from src.datasets.pdc import _list_records, match_gdacs_storm, names_agree


def _event(name, lat, lon, category="EVENT"):
    return {"name": name, "category": category, "latitude": lat, "longitude": lon}


# ---------------------------------------------------------------------------
# match_gdacs_storm — proximity matching
# ---------------------------------------------------------------------------


class TestMatchGdacsStorm:
    def test_matches_record_at_same_coordinates(self):
        # ONE-C-26 / Tropical Storm Lala, 2026-08-17: identical advisory
        # position on both sides.
        records = [_event("Tropical Storm Lala", 20.4, -163.4)]
        result = match_gdacs_storm(20.4, -163.4, records)
        assert result is not None
        assert result["record"]["name"] == "Tropical Storm Lala"
        assert result["distance_km"] == pytest.approx(0.0, abs=1.0)

    def test_matches_within_one_advisory_step(self):
        # ~200 km offset: one side is one 6-hour advisory behind.
        records = [_event("Tropical Storm Lala", 20.4, -163.4)]
        result = match_gdacs_storm(21.4, -162.0, records)
        assert result is not None
        assert result["distance_km"] == pytest.approx(180, abs=30)

    def test_no_match_beyond_threshold(self):
        # A different storm in the same basin is never this close.
        records = [_event("Typhoon Dolphin", 25.0, 135.0)]
        assert match_gdacs_storm(15.0, 145.0, records) is None

    def test_ignores_response_category_even_when_close(self):
        # RESPONSE records are analyst coordination snapshots, not events.
        records = [_event("Tropical Storm Bavi", 20.4, -163.4, category="RESPONSE")]
        assert match_gdacs_storm(20.4, -163.4, records) is None

    def test_prefers_nearest_of_multiple_candidates(self):
        records = [
            _event("Hurricane Far", 24.0, -160.0),
            _event("Tropical Storm Near", 20.5, -163.3),
        ]
        result = match_gdacs_storm(20.4, -163.4, records)
        assert result["record"]["name"] == "Tropical Storm Near"

    def test_matches_across_the_dateline(self):
        # WP/CP storms cross 180°; longitude wrap must not inflate distance.
        records = [_event("Typhoon Wrap", 15.0, -179.9)]
        result = match_gdacs_storm(15.0, 179.8, records)
        assert result is not None
        assert result["distance_km"] < 50

    def test_empty_records_returns_none(self):
        assert match_gdacs_storm(20.4, -163.4, []) is None


# ---------------------------------------------------------------------------
# names_agree — corroboration signal, not the matching key
# ---------------------------------------------------------------------------


class TestNamesAgree:
    def test_gdacs_suffix_and_pdc_status_words_are_stripped(self):
        assert names_agree("NANGKA-26", "Tropical Depression Nangka")
        assert names_agree("DOLPHIN-26", "Typhoon Dolphin")

    def test_designation_vs_assigned_name_disagrees(self):
        # The real ONE-C / Lala case: same storm, names cannot agree.
        assert not names_agree("ONE-C-26", "Tropical Storm Lala")

    def test_empty_name_never_agrees(self):
        assert not names_agree("", "Tropical Storm Lala")
        assert not names_agree("TROPICAL STORM", "Tropical Storm")


# ---------------------------------------------------------------------------
# _list_records — list-view parsing keeps the geometry
# ---------------------------------------------------------------------------


def _feature(name, lon, lat):
    return {
        "type": "Feature",
        "geometry": {"type": "Point", "coordinates": [lon, lat]},
        "properties": {"name": name, "category": "EVENT", "uuid": "u1"},
    }


class TestListRecords:
    def test_merges_point_geometry_into_properties(self):
        fc = {"type": "FeatureCollection", "features": [_feature("Lala", -163.4, 20.4)]}
        records = _list_records(fc)
        assert records == [
            {
                "name": "Lala",
                "category": "EVENT",
                "uuid": "u1",
                "latitude": 20.4,
                "longitude": -163.4,
            }
        ]

    def test_missing_geometry_raises(self):
        feat = _feature("Lala", -163.4, 20.4)
        feat["geometry"] = None
        fc = {"type": "FeatureCollection", "features": [feat]}
        with pytest.raises(ValueError, match="Lala"):
            _list_records(fc)

    def test_no_features_returns_empty_list(self):
        assert _list_records({"type": "FeatureCollection", "features": []}) == []
