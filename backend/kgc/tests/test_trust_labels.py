"""Tests for label_from_score boundary behaviour."""

from src.pipeline.trust.labels import label_from_score


class TestLabelFromScore:
    def test_zero_is_implausible(self):
        assert label_from_score(0.0) == "implausible"

    def test_below_lower_threshold_is_implausible(self):
        assert label_from_score(0.32) == "implausible"

    def test_lower_threshold_is_suspicious(self):
        # boundary is inclusive on the lower side
        assert label_from_score(0.33) == "suspicious"

    def test_middle_is_suspicious(self):
        assert label_from_score(0.5) == "suspicious"

    def test_below_upper_threshold_is_suspicious(self):
        assert label_from_score(0.66) == "suspicious"

    def test_upper_threshold_is_plausible(self):
        assert label_from_score(0.67) == "plausible"

    def test_one_is_plausible(self):
        assert label_from_score(1.0) == "plausible"

    def test_custom_thresholds(self):
        assert (
            label_from_score(0.4, suspicious_at=0.5, plausible_at=0.9) == "implausible"
        )
        assert (
            label_from_score(0.7, suspicious_at=0.5, plausible_at=0.9) == "suspicious"
        )
        assert (
            label_from_score(0.95, suspicious_at=0.5, plausible_at=0.9) == "plausible"
        )
