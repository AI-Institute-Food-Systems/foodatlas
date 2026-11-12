"""Per-pair counts and filter arrays in src.etl.bioactivity_aggregate."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd
from src.etl.bioactivity_aggregate import (
    MEASUREMENTS_CAP,
    aggregate_pair,
    source_kind,
)
from src.etl.materializer_bioactivity import materialize_food_bioactivity

_MODULE = "src.etl.materializer_bioactivity"

_FIELDS = [
    "bioactivity_metadata_id",
    "exhibit_type",
    "source_assay_id",
    "evidence_endpoint_type",
    "evidence_relation",
    "potency_value",
    "potency_unit",
    "efficacy_zeroactivity",
    "efficacy_infiniteactivity",
    "efficacy_logac50_value",
    "efficacy_hillslope",
    "evidence_type",
    "evidence_fit_r2",
    "evidence_fit_curveclass",
]


def _row(
    source: str | None,
    outcome: str = "Active",
    **fields: object,
) -> SimpleNamespace:
    return SimpleNamespace(
        **{**dict.fromkeys(_FIELDS), **fields},
        evidence_source=source,
        reported_activity_outcome=outcome,
    )


class TestSourceKind:
    def test_buckets(self):
        assert source_kind("Experimental") == "experimental"
        assert source_kind("predicted") == "predicted"
        assert source_kind("Computational") == "predicted"
        assert source_kind("curated") == ""
        assert source_kind(None) == ""


class TestAggregateSourceCounts:
    def test_counts_cover_rows_past_the_sample_cap(self):
        rows = [_row("Experimental")] * (MEASUREMENTS_CAP + 5) + [_row("predicted")] * 3
        agg = aggregate_pair(rows, {})
        assert len(agg["measurements"]) == MEASUREMENTS_CAP
        assert agg["measurement_count"] == MEASUREMENTS_CAP + 8
        assert agg["experimental_count"] == MEASUREMENTS_CAP + 5
        assert agg["predicted_count"] == 3

    def test_unknown_source_counts_toward_neither(self):
        agg = aggregate_pair([_row("curated"), _row(None)], {})
        assert agg["measurement_count"] == 2
        assert agg["experimental_count"] == 0
        assert agg["predicted_count"] == 0


class TestAggregateFilterArrays:
    def test_arrays_cover_rows_past_the_sample_cap(self):
        rows = [_row("Experimental", evidence_type="in vitro", potency_unit="uM")]
        rows *= MEASUREMENTS_CAP
        rows.append(_row("Experimental", evidence_type="in vivo", potency_unit="mg/kg"))
        agg = aggregate_pair(rows, {})
        assert agg["evidence_types"] == ["in vitro", "in vivo"]
        assert agg["units"] == ["mg/kg", "uM"]

    def test_missing_values_are_left_out(self):
        rows = [
            _row("Experimental", evidence_type=None, potency_unit=float("nan")),
            _row("Experimental", evidence_type="", potency_unit="uM"),
        ]
        agg = aggregate_pair(rows, {})
        assert agg["evidence_types"] == []
        assert agg["units"] == ["uM"]


class TestSampleCoverage:
    def test_sample_keeps_one_assay_per_value_past_the_cap(self):
        rows = [_row("Experimental", evidence_type="in vitro", potency_unit="uM")]
        rows *= MEASUREMENTS_CAP + 10
        rows.append(_row("predicted", evidence_type="in vivo", potency_unit="mg/kg"))
        sample = aggregate_pair(rows, {})["measurements"]
        assert len(sample) == MEASUREMENTS_CAP
        assert {m["evidence_type"] for m in sample} == {"in vitro", "in vivo"}
        assert {m["unit"] for m in sample} == {"mg/kg", "uM"}
        assert {m["evidence_source"] for m in sample} == {"Experimental", "predicted"}

    def test_sample_keeps_the_highest_value_per_value(self):
        rows = [
            _row("Experimental", evidence_type="in vitro", potency_value=float(i))
            for i in range(MEASUREMENTS_CAP)
        ]
        rows += [
            _row("Experimental", evidence_type="in vivo", potency_value=v)
            for v in (None, 2.0, 9.0, 5.0)
        ]
        sample = aggregate_pair(rows, {})["measurements"]
        in_vivo = [m["value"] for m in sample if m["evidence_type"] == "in vivo"]
        assert in_vivo == [9.0]

    def test_sample_keeps_original_order(self):
        rows = [
            _row("Experimental", evidence_type=et)
            for et in ["in vitro"] * MEASUREMENTS_CAP + ["in vivo"]
        ]
        rows.insert(0, rows.pop())
        sample = aggregate_pair(rows, {})["measurements"]
        assert sample[0]["evidence_type"] == "in vivo"
        assert len(sample) == MEASUREMENTS_CAP

    def test_small_pairs_keep_every_row(self):
        rows = [_row("Experimental"), _row("predicted")]
        assert len(aggregate_pair(rows, {})["measurements"]) == 2


class TestFoodBioactivityColumns:
    def test_writes_source_counts(self):
        merged = pd.DataFrame(
            [
                {**vars(_row("Experimental")), "head_id": "f1", "tail_id": "b1"},
                {**vars(_row("computational")), "head_id": "f1", "tail_id": "b1"},
            ]
        )
        with (
            patch(f"{_MODULE}._exploded_measurements", return_value=merged),
            patch(f"{_MODULE}.bulk_copy") as copy,
        ):
            materialize_food_bioactivity(MagicMock(), {"f1": "apple"}, {})
        df, cols = copy.call_args.args[2], copy.call_args.args[3]
        assert {
            "experimental_count",
            "predicted_count",
            "evidence_types",
            "units",
        } <= set(cols)
        assert df.loc[0, "experimental_count"] == 1
        assert df.loc[0, "predicted_count"] == 1
