"""Tests for evaluation precision/recall/F1 arithmetic."""

from src.pipeline.evaluation.metrics import (
    Counts,
    counts_from_adjudication,
    prf,
    summarize,
)
from src.pipeline.evaluation.schema import Adjudication, MatchedPair, Pair


def test_prf_basic() -> None:
    m = prf(Counts(tp=8, fp=2, fn=4))
    assert round(m.precision, 3) == 0.8
    assert round(m.recall, 3) == 0.667
    assert round(m.f1, 3) == 0.727


def test_prf_empty_denominator_is_zero() -> None:
    m = prf(Counts(0, 0, 0))
    assert (m.precision, m.recall, m.f1) == (0.0, 0.0, 0.0)


def test_counts_add() -> None:
    assert Counts(1, 2, 3) + Counts(4, 5, 6) == Counts(5, 7, 9)


def test_counts_from_adjudication_two_levels() -> None:
    adj = Adjudication(
        matches=[
            MatchedPair(food="apple", chemical="vitamin c", conc_agree=True),
            MatchedPair(food="pear", chemical="quercetin", conc_agree=False),
        ],
        model_only=[Pair(food="bogus", chemical="thing")],
        gold_only=[
            Pair(food="kale", chemical="lutein"),
            Pair(food="kale", chemical="iron"),
        ],
    )
    l1, l2 = counts_from_adjudication(adj)

    # Relation level: both matches are true positives.
    assert l1 == Counts(tp=2, fp=1, fn=2)
    # Strict level: the conc-mismatch match is both a false positive (wrong
    # amount) and a false negative (the correct fact was missed).
    assert l2 == Counts(tp=1, fp=2, fn=3)


def test_summarize_shape() -> None:
    out = summarize(Counts(tp=3, fp=1, fn=0))
    assert out["precision"] == 0.75
    assert out["recall"] == 1.0
    assert out["tp"] == 3
