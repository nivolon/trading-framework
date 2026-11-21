"""Indicators and strategies written as one function, and scripts importing each other."""
from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from tradingkit.core.script_ast import param_problems, resolve_params, script_params
from tradingkit.core.script_errors import ScriptError, run_script
from tradingkit.indicator import IndicatorContext, ScriptIndicator, preview_indicator_script
from tradingkit.script_api import (
    Signals,
    bundle,
    call_indicator,
    call_strategy,
    cross_above,
    cross_below,
    describe,
    is_function_script,
    project_imports,
    unbundle,
)
from tradingkit.strategy import run_strategy_script

EMA = '''
def custom_ema(source: Series, period: int = 3) -> Series:
    """A plain EMA."""
    out = np.empty_like(source)
    alpha = 2 / (period + 1)
    out[0] = source[0]
    for i in range(1, len(source)):
        out[i] = alpha * source[i] + (1 - alpha) * out[i - 1]
    return out
'''

DOUBLE = '''
from project.indicators import custom_ema

def double_ema(source: Series, period: int = 3) -> Series:
    return custom_ema(custom_ema(source, period), period)
'''

STRATEGY = '''
from project.indicators import custom_ema

def strategy(bars: Bars, fast: int = 2, slow: int = 4) -> Signals:
    f = custom_ema(bars.close, fast)
    s = custom_ema(bars.close, slow)
    return Signals(buy=cross_above(f, s), sell=cross_below(f, s))
'''


def _columns(close):
    close = np.asarray(close, dtype=np.float64)
    return {"timestamp": np.arange(len(close), dtype=np.int64) * 60, "open": close, "high": close + 1,
            "low": close - 1, "close": close, "volume": np.ones_like(close)}


def _candles(close):
    cols = _columns(close)
    return [{k: cols[k][i] for k in cols} for i in range(len(close))]


PRICES = [10, 9, 8, 7, 8, 10, 12, 13, 12, 10, 8, 7, 6, 7, 9, 11]


def test_a_function_script_is_told_apart_from_an_older_one():
    assert is_function_script(EMA)
    assert not is_function_script("result = ta.rsi(close, timeperiod=14)")
    assert not is_function_script("def helper(x):\n    return x\n\nresult = helper(close)")
    assert not is_function_script("for i in range(3):\n    signals.append({})\ndef f():\n    pass")


def test_parameters_come_from_the_signature():
    params = script_params(EMA)
    assert params["period"] == {"type": "int", "default": 3}
    assert params["source"]["default"] == "close" and "hl2" in params["source"]["options"]
    assert resolve_params(EMA, {"period": "5"}) == {"source": "close", "period": 5}
    assert "bars" not in script_params(STRATEGY)
    assert script_params(STRATEGY)["slow"] == {"type": "int", "default": 4}


def test_describe_lists_inputs_parameters_output_and_imports():
    info = describe(DOUBLE)
    assert info["name"] == "double_ema"
    assert info["inputs"] == [{"name": "source", "kind": "series"}]
    assert info["params"] == [{"name": "period", "type": "int", "default": 3}]
    assert info["output"] == "Series"
    assert info["imports"] == [{"type": "indicator", "name": "custom_ema"}]
    assert describe("result = close") is None


def test_an_indicator_reads_the_series_picked():
    cols = _columns(PRICES)
    on_close = call_indicator(EMA, cols, {"period": 3})
    on_high = call_indicator(EMA, cols, {"period": 3, "source": "high"})
    assert np.allclose(on_high - on_close, 1.0)
    hl2 = call_indicator(EMA, cols, {"source": "hl2"})
    assert np.allclose(hl2, call_indicator(EMA, cols, {}))


def test_an_import_of_the_project_uses_the_bundled_script():
    cols = _columns(PRICES)
    code = bundle(DOUBLE, {"indicators.custom_ema": EMA})
    once = call_indicator(EMA, cols, {"period": 3})
    twice = call_indicator(code, cols, {"period": 3})
    assert np.allclose(twice, call_indicator(EMA, {**cols, "close": once}, {"period": 3}))


def test_bundling_keeps_the_scripts_line_numbers():
    code = bundle("x = 1\ny = undefined_name\n", {"indicators.a": EMA})
    assert unbundle(code)[0].count("\n") == 2
    with pytest.raises(ScriptError) as err:
        run_script(code, "<s>", {})
    assert err.value.line == 2


def test_importing_a_script_the_project_lacks_says_so():
    with pytest.raises(ScriptError, match="no indicator 'custom_ema'"):
        call_indicator(DOUBLE, _columns(PRICES), {})


def test_importing_an_older_script_says_how_to_make_it_importable():
    code = bundle(DOUBLE, {"indicators.custom_ema": "result = close"})
    with pytest.raises(ScriptError, match="written the older way"):
        call_indicator(code, _columns(PRICES), {})


def test_imports_do_not_leak_into_the_next_run():
    call_indicator(bundle(DOUBLE, {"indicators.custom_ema": EMA}), _columns(PRICES), {})
    with pytest.raises(ScriptError):
        call_indicator(DOUBLE, _columns(PRICES), {})


def test_builtin_indicators_import_by_name():
    code = '''
from builtin.indicators import sma

def smooth(source: Series, period: int = 3) -> Series:
    return sma(source, timeperiod=period)
'''
    talib = pytest.importorskip("talib")
    cols = _columns(PRICES)
    assert np.allclose(call_indicator(code, cols, {}), talib.SMA(cols["close"], timeperiod=3), equal_nan=True)


def test_an_older_script_can_import_a_function_indicator():
    code = bundle("from project.indicators import custom_ema\nresult = custom_ema(close, 3)\n",
                  {"indicators.custom_ema": EMA})
    namespace = {"close": np.asarray(PRICES, dtype=np.float64), "np": np}
    run_script(code, "<s>", namespace)
    assert np.allclose(namespace["result"], call_indicator(EMA, _columns(PRICES), {}))


def test_a_function_strategy_gives_signals():
    code = bundle(STRATEGY, {"indicators.custom_ema": EMA})
    events = call_strategy(code, _columns(PRICES), {})
    kinds = [kind for _, kind in events]
    assert "buy" in kinds and "sell" in kinds
    signals = run_strategy_script(code, _candles(PRICES), {"fast": 2}, name="x")
    assert [s["signal_type"] for s in signals] == kinds
    assert signals[0]["price"] == PRICES[events[0][0]]


def test_signals_check_their_length():
    with pytest.raises(ValueError, match="3 values for 2 bars"):
        Signals(buy=[True, False, True]).events(2)


def test_crosses():
    a = np.array([1, 2, 3, 2, 1], dtype=float)
    assert cross_above(a, 2).tolist() == [False, False, True, False, False]
    assert cross_below(a, 2).tolist() == [False, False, False, False, True]


def test_the_chart_runs_a_function_indicator():
    df = pl.DataFrame(_columns(PRICES))
    values = ScriptIndicator(code=EMA, period=3, source="close").compute(IndicatorContext(df))
    assert np.allclose(values, call_indicator(EMA, _columns(PRICES), {"period": 3}))


def test_the_editor_previews_a_function_indicator_with_several_lines():
    import io
    code = '''
def bands(source: Series, width: float = 1.0) -> dict:
    return {"upper": source + width, "lower": source - width}
'''
    buffer = io.BytesIO()
    pl.DataFrame(_columns(PRICES)).write_ipc(buffer)
    preview = preview_indicator_script(code, {"width": 2.0}, buffer.getvalue())
    assert preview["type"] == "multi" and preview["columns"] == ["upper", "lower"]
    assert preview["rows"][0]["upper"] == PRICES[0] + 2


def test_project_imports_are_read_without_running():
    assert project_imports("from project.indicators import a, b\nfrom project.strategies.c import x") == [
        {"type": "indicator", "name": "a"}, {"type": "indicator", "name": "b"}, {"type": "strategy", "name": "c"},
    ]


def test_a_signature_is_not_reported_as_a_mistake():
    assert param_problems(EMA) == []
    assert param_problems(STRATEGY) == []
