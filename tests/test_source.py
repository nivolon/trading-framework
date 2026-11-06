"""tradingkit.source — the DataSource contract and ScriptSource."""
from __future__ import annotations

import asyncio
import pickle

import aiohttp
import polars as pl
import pytest

from tradingkit.executor import LocalExecutor, SubprocessExecutor
from tradingkit.executor.base import PluginExecutor
from tradingkit.source import DataSource, ScriptSource, Stream, TransientSourceError

SCRIPT = """
timestamp_unit = "ms"
__params__ = {"batch_size": {"type": "int", "default": 1440, "label": "Batch size"}}

class Demo(DataSource):
    streams = {
        "candles": Stream(table="unit_demo", step=60, columns={"timestamp": int}),
        "trades": Stream(table="trades", updates="append", history=False),
    }

    async def setup(self):
        self.setups = getattr(self, "setups", 0) + 1

    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return [{"timestamp": start_ts, "close": float(self.setups), "size": self.config["batch_size"]}]

    async def live(self, stream, symbol):
        for i in range(2):
            yield {"timestamp": i, "symbol": symbol}
        raise OSError("dropped")
"""


# ------------------------------------------------------------------ #
# Lifecycle                                                            #
# ------------------------------------------------------------------ #

class Counting(DataSource):
    setups = 0
    closes = 0

    async def setup(self):
        self.setups += 1

    async def close(self):
        self.closes += 1

    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return [{"timestamp": start_ts}]


async def test_setup_runs_once_across_calls():
    src = Counting()
    await src.get_historical_data("BTC", 60, 0, 60)
    await src.get_historical_data("BTC", 60, 60, 120)
    assert src.setups == 1


async def test_shutdown_closes_once_and_allows_a_new_setup():
    src = Counting()
    await src.open()
    await src.shutdown()
    await src.shutdown()
    assert src.closes == 1
    await src.open()
    assert src.setups == 2


async def test_session_is_shared_and_closed():
    src = Counting()
    first = src.session
    assert src.session is first
    await src.shutdown()
    assert first.closed


def test_a_used_source_stays_picklable():
    src = Counting()
    src._ready = True
    assert pickle.loads(pickle.dumps(src))._ready is False


async def test_a_list_result_becomes_a_dataframe():
    df = await Counting().get_historical_data("BTC", 60, 5, 60)
    assert df.to_dicts() == [{"timestamp": 5}]


# ------------------------------------------------------------------ #
# Retries                                                              #
# ------------------------------------------------------------------ #

class Flaky(DataSource):
    def __init__(self, failures, error=TransientSourceError("busy")):
        super().__init__({"retry_delay_s": 0, "max_retries": 3})
        self.failures = failures
        self.error = error
        self.calls = 0

    async def history(self, stream, symbol, start_ts, end_ts, limit):
        self.calls += 1
        if self.calls <= self.failures:
            raise self.error
        return [{"timestamp": start_ts}]


async def test_transient_failures_are_retried():
    src = Flaky(failures=2)
    df = await src.get_historical_data("BTC", 60, 0, 60)
    assert src.calls == 3 and df.height == 1


async def test_network_errors_are_retried():
    src = Flaky(failures=1, error=aiohttp.ClientError("reset"))
    await src.get_historical_data("BTC", 60, 0, 60)
    assert src.calls == 2


async def test_retries_are_bounded():
    src = Flaky(failures=10)
    with pytest.raises(TransientSourceError):
        await src.get_historical_data("BTC", 60, 0, 60)
    assert src.calls == 3


async def test_plugin_bugs_are_not_retried():
    src = Flaky(failures=10, error=KeyError("oops"))
    with pytest.raises(KeyError):
        await src.get_historical_data("BTC", 60, 0, 60)
    assert src.calls == 1


async def test_every_attempt_is_rate_limited():
    class Limited(Flaky):
        limited = 0

        async def rate_limit(self):
            self.limited += 1

    src = Limited(failures=1)
    await src.get_historical_data("BTC", 60, 0, 60)
    assert src.limited == 2


# ------------------------------------------------------------------ #
# Reconnecting                                                         #
# ------------------------------------------------------------------ #

class Dropping(DataSource):
    def __init__(self, script):
        super().__init__({"reconnect_delay_s": 0, "stale_timeout_s": 0.05})
        self.script = script
        self.connects = 0

    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return []

    async def live(self, stream, symbol):
        step = self.script[self.connects]
        self.connects += 1
        for row in step["rows"]:
            yield row
        if step.get("hang"):
            await asyncio.sleep(10)
        if "raises" in step:
            raise step["raises"]


async def _collect(src, count):
    rows, events = [], []
    stream = src.stream("BTC", 60, on_event=events.append)
    async for row in stream:
        rows.append(row)
        if len(rows) == count:
            break
    await stream.aclose()
    return rows, events


async def test_a_dropped_connection_is_reconnected():
    src = Dropping([{"rows": [1], "raises": OSError("reset")}, {"rows": [2]}])
    rows, events = await _collect(src, 2)
    assert rows == [1, 2]
    assert [e["event"] for e in events] == ["reconnecting", "connected"]
    assert "reset" in events[0]["error"]


async def test_a_connection_that_ends_is_reconnected():
    src = Dropping([{"rows": [1]}, {"rows": [2]}])
    rows, _ = await _collect(src, 2)
    assert rows == [1, 2]


async def test_a_silent_connection_counts_as_dead():
    src = Dropping([{"rows": [1], "hang": True}, {"rows": [2]}])
    rows, events = await _collect(src, 2)
    assert rows == [1, 2]
    assert "no data" in events[0]["error"]


async def test_a_source_without_connect_does_not_loop():
    with pytest.raises(NotImplementedError):
        await _collect(Counting(), 1)


def test_live_support_follows_the_class():
    assert Counting().has_live() is False
    assert Dropping([]).has_live() is True


# ------------------------------------------------------------------ #
# Streams                                                              #
# ------------------------------------------------------------------ #

class TwoStreams(DataSource):
    streams = {
        "candles": Stream(table="candles", step=60, columns={"timestamp": int, "close": float}),
        "trades":  Stream(table="trades", updates="append", history=False),
    }

    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return [{"timestamp": start_ts, "close": 1.0, "from": stream}]


def test_timeframe_selects_the_stream_with_that_step():
    assert TwoStreams().resolve_stream(timeframe=60) == "candles"


def test_the_first_stream_is_the_default():
    assert TwoStreams().resolve_stream() == "candles"


def test_unknown_streams_are_rejected():
    with pytest.raises(ValueError, match="Unknown stream"):
        TwoStreams().resolve_stream("funding")


def test_a_source_without_streams_still_works():
    assert Counting().resolve_stream() == "default"
    assert Counting().stream_spec().step is None


async def test_the_named_stream_reaches_the_plugin():
    df = await TwoStreams().get_historical_data("BTC", 60, 0, 60, stream="candles")
    assert df["from"].to_list() == ["candles"]


async def test_rows_missing_a_declared_column_are_rejected():
    class Broken(TwoStreams):
        async def history(self, stream, symbol, start_ts, end_ts, limit):
            return [{"timestamp": start_ts}]

    with pytest.raises(ValueError, match="close"):
        await Broken().get_historical_data("BTC", 60, 0, 60)


def test_stream_declarations_are_read_without_running_the_script():
    specs = ScriptSource(f"raise SystemExit('executed')\n{SCRIPT}").stream_specs()
    assert specs["candles"] == Stream(table="unit_demo", step=60, columns={"timestamp": "int"})
    assert specs["trades"].updates == "append" and specs["trades"].history is False


# ------------------------------------------------------------------ #
# ScriptSource                                                         #
# ------------------------------------------------------------------ #

def test_declarations_are_read_without_running_the_script():
    src = ScriptSource(f"raise SystemExit('executed')\n{SCRIPT}")
    assert src.stream_spec("candles").table == "unit_demo"
    assert src.timestamp_unit == "ms"
    assert src.has_live() is True
    assert src.extract_params()["batch_size"]["default"] == 1440


def test_config_from_a_form_is_cast_to_the_declared_type():
    assert ScriptSource(SCRIPT, config={"batch_size": "720"}).merged_config() == {"batch_size": 720}


def test_config_overrides_defaults():
    assert ScriptSource(SCRIPT, config={"batch_size": 5}).merged_config() == {"batch_size": 5}
    assert ScriptSource(SCRIPT).merged_config() == {"batch_size": 1440}


async def test_script_gets_its_merged_config_and_runs_setup_once():
    src = ScriptSource(SCRIPT, config={"batch_size": 5})
    await src.get_historical_data("BTC", 60, 0, 60)
    df = await src.get_historical_data("BTC", 60, 60, 120)
    assert df.to_dicts() == [{"timestamp": 60, "close": 1.0, "size": 5}]


async def test_a_script_without_a_class_is_rejected():
    with pytest.raises(ValueError, match="DataSource subclass"):
        await ScriptSource("result = 1").get_historical_data("BTC", 60, 0, 60)


async def test_the_most_derived_class_wins():
    code = """
class Base(DataSource):
    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return [{"timestamp": 1}]

class Concrete(Base):
    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return [{"timestamp": 2}]
"""
    df = await ScriptSource(code).get_historical_data("BTC", 60, 0, 60)
    assert df["timestamp"].to_list() == [2]


async def test_a_class_with_its_own_init_needs_no_config_argument():
    code = """
class NoConfig(DataSource):
    def __init__(self):
        super().__init__({"max_retries": 1})

    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return [{"timestamp": self.config["max_retries"]}]
"""
    df = await ScriptSource(code).get_historical_data("BTC", 60, 0, 60)
    assert df["timestamp"].to_list() == [1]


async def test_an_abstract_class_raises_clearly():
    with pytest.raises(TypeError):
        await ScriptSource("class Bad(DataSource):\n    pass").get_historical_data("BTC", 60, 0, 60)


def test_timeframe_helpers_are_available_to_scripts():
    code = "class S(DataSource):\n    label = tf_string(60)\n    async def history(self, *a): return []"
    assert ScriptSource(code)._instance().label == "1m"


def test_a_prepared_source_is_picklable():
    src = ScriptSource(SCRIPT)
    src._instance()
    restored = pickle.loads(pickle.dumps(src))
    assert restored._impl is None and restored._code == SCRIPT


# ------------------------------------------------------------------ #
# Sessions                                                             #
# ------------------------------------------------------------------ #

async def test_base_executor_session_keeps_one_instance():
    session = await LocalExecutor().open_source(ScriptSource(SCRIPT))
    await session.fetch("BTC", 60, 0, 60)
    df = await session.fetch("BTC", 60, 60, 120)
    await session.close()
    assert df["close"].to_list() == [1.0]
    assert isinstance(LocalExecutor(), PluginExecutor)


async def test_worker_keeps_state_between_calls_in_another_process():
    code = """
import os

class Stateful(DataSource):
    async def setup(self):
        self.setups = getattr(self, "setups", 0) + 1

    async def history(self, stream, symbol, start_ts, end_ts, limit):
        print("plugin output must not break the protocol")
        return [{"timestamp": start_ts, "pid": os.getpid(), "setups": self.setups}]
"""
    import os

    session = await SubprocessExecutor().open_source(ScriptSource(code))
    try:
        first  = (await session.fetch("BTC", 60, 0, 60)).to_dicts()[0]
        second = (await session.fetch("BTC", 60, 60, 120)).to_dicts()[0]
    finally:
        await session.close()
    assert first["pid"] == second["pid"] != os.getpid()
    assert second["setups"] == 1


async def test_worker_streams_and_reports_reconnects():
    session = await SubprocessExecutor().open_source(
        ScriptSource(SCRIPT, config={"reconnect_delay_s": 0}),
    )
    rows, events = [], []
    stream = session.stream("BTC", 60, on_event=events.append)
    async for row in stream:
        rows.append(row)
        if len(rows) == 3:
            break
    await stream.aclose()
    await session.close()
    assert [r["timestamp"] for r in rows] == [0, 1, 0]
    assert events[0]["event"] == "reconnecting"


async def test_worker_surfaces_plugin_errors():
    code = "class S(DataSource):\n    async def history(self, *a):\n        raise KeyError('boom')"
    session = await SubprocessExecutor().open_source(ScriptSource(code))
    with pytest.raises(RuntimeError, match="boom"):
        await session.fetch("BTC", 60, 0, 60)
    await session.close()


async def test_a_stream_resumes_when_its_worker_dies():
    code = """
import asyncio

class Ticking(DataSource):
    async def history(self, *a):
        return []

    async def live(self, stream, symbol):
        while True:
            yield {"timestamp": 1}
            await asyncio.sleep(0.01)
"""
    session = await SubprocessExecutor().open_source(ScriptSource(code))
    events, rows = [], 0
    stream = session.stream("BTC", 60, on_event=events.append)
    async for _ in stream:
        rows += 1
        if rows == 1:
            session._proc.kill()
        if "connected" in [e["event"] for e in events]:
            break
    await stream.aclose()
    await session.close()
    assert [e["event"] for e in events] == ["reconnecting", "connected"]


async def test_worker_limits_compute_threads(monkeypatch):
    from tradingkit.executor.source_worker import _worker_env

    monkeypatch.setenv("TRADINGKIT_SOURCE_WORKER_THREADS", "2")
    env = _worker_env()
    assert env["POLARS_MAX_THREADS"] == "2" and env["OPENBLAS_NUM_THREADS"] == "2"


async def test_worker_restarts_after_it_dies():
    code = """
import os

class S(DataSource):
    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return [{"timestamp": start_ts, "pid": os.getpid()}]
"""
    session = await SubprocessExecutor().open_source(ScriptSource(code))
    first = (await session.fetch("BTC", 60, 0, 60))["pid"][0]
    session._proc.kill()
    await session._proc.wait()
    await session._reader
    second = (await session.fetch("BTC", 60, 0, 60))["pid"][0]
    await session.close()
    assert first != second


# ------------------------------------------------------------------ #
# Gap recovery defaults / unit-agnosticism                             #
# ------------------------------------------------------------------ #

class _Dummy(DataSource):
    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return pl.DataFrame({"timestamp": [start_ts], "close": [1.0]})


async def test_detect_gaps_finds_missing_bars():
    rows = [{"timestamp": 0}, {"timestamp": 60}, {"timestamp": 300}]
    assert await _Dummy().detect_gaps(rows, 60) == [
        {"start_timestamp": 120, "end_timestamp": 240, "missing_rows": 3},
    ]


async def test_detect_gaps_tolerates_duplicate_and_backwards_timestamps():
    """A source re-pushing the same still-forming bar must not look like a gap."""
    rows = [{"timestamp": 60}, {"timestamp": 60}, {"timestamp": 55}, {"timestamp": 120}]
    assert await _Dummy().detect_gaps(rows, 60) == []


async def test_detect_gaps_is_unit_agnostic():
    """Same shape of data in milliseconds yields the same gaps -- the arithmetic never
    assumes seconds."""
    sec = [{"timestamp": 0}, {"timestamp": 60}, {"timestamp": 300}]
    ms  = [{"timestamp": t["timestamp"] * 1000} for t in sec]
    gaps_s  = await _Dummy().detect_gaps(sec, 60)
    gaps_ms = await _Dummy().detect_gaps(ms, 60_000)
    assert gaps_ms == [{k: v * 1000 if k != "missing_rows" else v for k, v in g.items()}
                       for g in gaps_s]


def test_batch_gaps_merges_close_gaps():
    source = _Dummy({"gap_max_batch": 1000, "gap_max_time_gap": 3600})
    batches = source.batch_gaps([
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 10},
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 10},
    ])
    assert batches == [{"start_timestamp": 0, "end_timestamp": 180, "total_rows": 20}]


def test_batch_gaps_splits_when_over_max_batch():
    source = _Dummy({"gap_max_batch": 15, "gap_max_time_gap": 3600})
    assert len(source.batch_gaps([
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 10},
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 10},
    ])) == 2


def test_batch_gaps_splits_when_too_far_apart():
    source = _Dummy({"gap_max_batch": 1000, "gap_max_time_gap": 100})
    assert len(source.batch_gaps([
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 1},
        {"start_timestamp": 10_000, "end_timestamp": 10_060, "missing_rows": 1},
    ])) == 2


def test_batch_gaps_sorts_unordered_input():
    source = _Dummy({"gap_max_batch": 1000, "gap_max_time_gap": 3600})
    batches = source.batch_gaps([
        {"start_timestamp": 120, "end_timestamp": 180, "missing_rows": 1},
        {"start_timestamp": 0, "end_timestamp": 60, "missing_rows": 1},
    ])
    assert batches[0]["start_timestamp"] == 0


def test_batch_gaps_empty_input():
    assert _Dummy().batch_gaps([]) == []


async def test_fetch_gap_returns_real_rows():
    rows = await _Dummy().fetch_gap("BTC", 60, 120, 240)
    assert rows == [{"timestamp": 120, "close": 1.0}]


async def test_rate_limit_throttles_by_min_delay():
    import time as _time
    src = _Dummy({"rate_limit_ms": 60})
    await src.rate_limit()
    started = _time.monotonic()
    await src.rate_limit()
    assert _time.monotonic() - started >= 0.03
