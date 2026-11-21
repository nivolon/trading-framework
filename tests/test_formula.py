"""FORMULA: a synthetic market as one line, parsed and computed, never executed as code."""
from __future__ import annotations

import functools

import polars as pl
import pytest

from tradingkit.aggregation import AggregationWorker, describe_cross_source
from tradingkit.formula import evaluate, parse


def frame(closes, highs=None, start=0):
    return pl.DataFrame({
        "timestamp": [start + i * 60 for i in range(len(closes))],
        "close": [float(c) for c in closes],
        "high": [float(h) for h in (highs or closes)],
        "symbol": ["X"] * len(closes),
    })


def test_names_are_series_case_insensitively_in_order():
    formula = parse("SE3 - dk1 + se3")
    assert formula.names == ["se3", "dk1"] and formula.warmup == 0


def test_the_widest_window_is_the_warmup():
    assert parse("zscore(a - b, 96) + mean(a, 4)").warmup == 96


@pytest.mark.parametrize("text, message", [
    ("", "empty"),
    ("a +", "cannot be read"),
    ("__import__('os')", "No function __import__"),
    ("a.__class__", "not a column"),
    ("open('x')", "No function open"),
    ("a if b else c", "cannot use"),
    ("mean(a, b)", "whole number"),
    ("mean(a, 0)", "whole number"),
    ("abs(a, b)", "one value"),
    ("[a, b]", "cannot use"),
    ("a + mean", "is a function"),
    ("3 + 4", "names no series"),
])
def test_what_is_not_a_formula_is_refused(text, message):
    with pytest.raises(ValueError, match=message):
        parse(text)


def test_series_align_on_time_and_columns_are_picked_by_name():
    a = frame([10, 11, 12, 13], highs=[11, 12, 13, 14])
    b = frame([5, 6, 7], start=60)
    out = evaluate(parse("a.high - b"), {"a": a, "b": b})
    assert out["timestamp"].to_list() == [60, 120, 180]
    assert out["value"].to_list() == [7.0, 7.0, 7.0]


def test_functions_across_series_and_over_windows():
    a, b = frame([1, 2, 3, 4]), frame([3, 4, 5, 6])
    assert evaluate(parse("avg(a, b)"), {"a": a, "b": b})["value"].to_list() == [2.0, 3.0, 4.0, 5.0]
    assert evaluate(parse("change(a, 2)"), {"a": a})["value"].to_list() == [2.0, 2.0]
    assert evaluate(parse("max(a, b) / min(a, b)"), {"a": a, "b": b})["value"].to_list()[0] == 3.0
    z = evaluate(parse("zscore(a, 3)"), {"a": a})["value"].to_list()
    assert len(z) == 2 and z[0] == pytest.approx(1.0)


def test_rows_that_are_not_numbers_are_left_out():
    out = evaluate(parse("a / b"), {"a": frame([1, 2]), "b": frame([0, 1])})
    assert out["value"].to_list() == [2.0]


def test_a_missing_column_is_named():
    with pytest.raises(ValueError, match="a.low"):
        evaluate(parse("a.low"), {"a": frame([1, 2])})


def test_a_formula_script_declares_its_roles():
    described = describe_cross_source('FORMULA = "zscore(SE3 - DK1, 96)"')
    assert described["inputs"] == ["se3", "dk1"] and described["has_combine"] and described["formula"]


async def test_a_formula_run_reads_its_roles_with_warmup():
    from tests.test_aggregation_worker import CONNECTIONS, FakeDb, run_row  # noqa: F401
    code = 'FORMULA = "mean(a, 2) - b"'
    db = FakeDb([("alice", "f.py", code), run_row("alice", "spread", "f.py", {"a": "eth", "b": "btc"})])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    agg = await worker.describe_run("alice", "spread", code, {"a": "eth", "b": "btc"})
    rows = await worker._executor.call(
        "tradingkit.aggregation:run_cross_source_combine",
        {"code": code, "start_ts": 60, "end_ts": 200, "source_filters": {}},
        on_request=functools.partial(worker.serve, inputs=agg.bound_inputs),
    )
    assert db.warmups == [("candles", 59, 2), ("candles", 59, 2)]
    assert [r["timestamp"] for r in rows] == [60, 120]
