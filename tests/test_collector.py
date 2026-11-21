"""tradingkit.collector — _ConnectionWorker gap/date logic and its use of the source's
hooks, plus DataCollector lifecycle."""
from __future__ import annotations

import time

import polars as pl
import pytest

from tradingkit.collector import DataCollector, _ConnectionWorker
from tradingkit.core.timeframe import UNIT_SCALE
from tradingkit.executor.base import SourceSession
from tradingkit.source import ScriptSource

# ------------------------------------------------------------------ #
# _ConnectionWorker — date math + gap detection with a fake db          #
# ------------------------------------------------------------------ #

def _make_conn(**overrides) -> dict:
    conn = {
        "name": "wb_btc_1m", "source": "whitebit.py", "symbol": "BTC_USDT",
        "timeframe": "1m", "start_date": "2020-01-01T00:00:00Z", "config": "{}",
    }
    conn.update(overrides)
    return conn


class FakeSource(ScriptSource):
    """A real ScriptSource (hence a real DataSource), so the worker exercises the
    inherited batch_gaps/rate_limit rather than a hand-rolled stand-in. Every hook the
    worker is supposed to delegate to records that it was called."""

    def __init__(self, config: dict | None = None, timestamp_unit: str = "s"):
        super().__init__(
            code=(
                "class S(DataSource):\n"
                "    streams = {'unit': Stream(table='unit_test', step=step)}\n"
                "    async def history(self, *a): return []\n"
            ).replace("step=step", f"step={60 * UNIT_SCALE[timestamp_unit]}"),
            config=config,
        )
        self._unit = timestamp_unit
        self.calls: list[str] = []

    @property
    def timestamp_unit(self) -> str:
        return self._unit

    def batch_gaps(self, gaps):
        self.calls.append("batch_gaps")
        return super().batch_gaps(gaps)

    async def get_historical_data(self, symbol, timeframe, start_ts, end_ts, limit=5000, stream=None):
        self.calls.append("get_historical_data")
        return pl.DataFrame({"timestamp": [start_ts], "close": [1.0]})


class RecordingSession(SourceSession):
    def __init__(self, source, calls):
        super().__init__(source)
        self.calls = calls

    async def fetch(self, symbol, timeframe, start_ts, end_ts, limit=5000, stream=None):
        self.calls.append((symbol, timeframe, start_ts, end_ts))
        return await super().fetch(symbol, timeframe, start_ts, end_ts, limit, stream)


class FakeExecutor:
    """Stands in for LocalExecutor/SubprocessExecutor — records that the worker fetches
    through its session rather than calling the source directly."""

    def __init__(self):
        self.calls: list[tuple] = []
        self.opened = 0

    async def open_source(self, source):
        self.opened += 1
        return RecordingSession(source, self.calls)


class FakeDb:
    def __init__(self):
        self.unit_range = (None, None)
        self.stored_rows = 0
        self.gaps = []
        self.inserted: list[pl.DataFrame] = []

    async def get_unit_range(self, table, exchange, symbol, timeframe):
        return self.unit_range

    async def find_unit_gaps(self, table, exchange, symbol, timeframe, start_ts, end_ts, unit_interval_s=60):
        return self.gaps

    async def count_unit_rows(self, table, exchange, symbol, timeframe, start_ts, end_ts):
        return self.stored_rows

    async def invalidate_derived(self, exchange, symbol, start_ts):
        pass

    async def ensure_raw_table(self, table, schema, updates="replace"):
        pass

    async def insert_unit_batch(self, table, df, exchange, symbol, timeframe):
        self.inserted.append(df)

    async def flush(self):
        pass


def _async_return(value):
    """Stand-in for an async method that just returns a fixed value, for monkeypatching
    _recover_gaps in the gap-cycle tests below without needing a full fake data pipeline."""
    async def _fn():
        return value
    return _fn


def _make_worker(source=None, db=None, **conn_overrides) -> _ConnectionWorker:
    return _ConnectionWorker(
        _make_conn(**conn_overrides),
        db=db or FakeDb(),
        source=source or FakeSource(),
        executor=FakeExecutor(),
    )


@pytest.fixture
def worker():
    return _make_worker()


def test_parse_start_ts_aligns_to_interval(worker):
    ts = worker._parse_start_ts()
    assert ts % 60 == 0


def test_safe_end_is_at_least_one_interval_before_now(worker):
    now = int(time.time())
    end = worker._safe_end()
    assert end <= now - 60
    assert end % 60 == 0


# ------------------------------------------------------------------ #
# The worker delegates to the source's hooks (not its own copies)      #
# ------------------------------------------------------------------ #

async def test_fetch_and_store_goes_through_the_executor():
    """User script code must run via the executor so a SubprocessExecutor can isolate
    it — the worker used to call source.historical() directly, unsandboxed."""
    source, db = FakeSource(), FakeDb()
    worker = _ConnectionWorker(
        _make_conn(), db=db, source=source, executor=FakeExecutor(),
    )
    stored = await worker._fetch_and_store(0, 600, 1, 1)

    assert worker._executor.calls == [("BTC_USDT", 60, 0, 600)]
    assert stored == 1
    assert len(db.inserted) == 1


async def test_one_session_serves_every_fetch():
    source, db = FakeSource(), FakeDb()
    worker = _ConnectionWorker(_make_conn(), db=db, source=source, executor=FakeExecutor())
    await worker._fetch_and_store(0, 600, 1, 1)
    await worker._fetch_and_store(600, 1200, 1, 1)
    assert worker._executor.opened == 1


def test_stream_events_drive_the_connection_status():
    worker = _make_worker()
    worker._on_stream_event({"event": "reconnecting", "error": "reset"})
    assert worker.progress["status"] == "reconnecting"
    assert worker.progress["error"] == "reset"
    assert not worker._gap_wake.is_set()

    worker._on_stream_event({"event": "connected"})
    assert worker.progress["status"] == "streaming"
    assert worker.progress["error"] is None
    assert worker._gap_wake.is_set()


async def test_recover_gaps_batches_through_the_source():
    """Batching is the source's business (its exchange's request limits), detection is
    the worker's (ClickHouse knows what is actually missing)."""
    source, db = FakeSource(), FakeDb()
    db.unit_range = (0, 10_000_000_000)          # window fully covered except the middle
    db.gaps = [{"start_timestamp": 300, "end_timestamp": 360, "missing_rows": 1}]
    worker = _ConnectionWorker(
        _make_conn(), db=db, source=source, executor=FakeExecutor(),
    )
    await worker._recover_gaps()
    assert "batch_gaps" in source.calls


async def test_worker_has_no_private_rate_limit_or_batcher():
    """Regression: these moved onto DataSource; leaving stale copies behind would mean
    two sources of truth for the same exchange-level settings."""
    worker = _make_worker()
    assert not hasattr(worker, "_rate_limit")
    assert not hasattr(worker, "_batch_gaps")
    assert not hasattr(worker, "_gap_batcher")


# ------------------------------------------------------------------ #
# Unit-agnostic wall-clock conversion                                  #
# ------------------------------------------------------------------ #

def test_rows_are_labelled_1m_whatever_the_connection_asks_for():
    # Regression: a "1h" connection stored its minute bars under "1h", so that label held
    # 60s-spaced rows and the chart drew them as hourly candles.
    assert _make_worker(timeframe="1h")._timeframe == "1m"


def test_unit_interval_follows_source_timestamp_unit():
    worker = _make_worker(source=FakeSource(timestamp_unit="ms"))
    assert worker._unit_interval == 60_000


def test_safe_end_scales_to_millisecond_source():
    """Wall-clock time is the one place the framework must know the unit — gap and
    batching arithmetic stays unit-relative."""
    worker = _make_worker(source=FakeSource(timestamp_unit="ms"))
    now_ms = int(time.time() * 1000)
    end = worker._safe_end()
    assert end % 60_000 == 0
    assert now_ms - 2 * 60_000 <= end <= now_ms


def test_parse_start_ts_scales_to_millisecond_source():
    sec_worker = _make_worker()
    ms_worker  = _make_worker(source=FakeSource(timestamp_unit="ms"))
    assert ms_worker._parse_start_ts() == sec_worker._parse_start_ts() * 1000


async def test_detect_gaps_no_data_at_all(worker):
    worker._db.unit_range = (None, None)
    gaps = await worker._detect_gaps(0, 600)
    assert len(gaps) == 1
    assert gaps[0]["start_timestamp"] == 0
    assert gaps[0]["end_timestamp"] == 600


async def test_detect_gaps_missing_at_start_and_end(worker):
    worker._db.unit_range = (300, 600)  # data covers [300, 600], window is [0, 900]
    worker._db.gaps = []
    gaps = await worker._detect_gaps(0, 900)
    starts = [g["start_timestamp"] for g in gaps]
    assert 0 in starts        # missing before min_ts
    assert 660 in starts      # missing after max_ts (max_ts + interval)


async def test_detect_gaps_fully_covered_returns_nothing(worker):
    worker._db.unit_range = (0, 900)
    worker._db.gaps = []
    gaps = await worker._detect_gaps(0, 900)
    assert gaps == []


async def test_detect_gaps_includes_middle_gaps_from_db(worker):
    worker._db.unit_range = (0, 900)
    worker._db.gaps = [{"start_timestamp": 300, "end_timestamp": 360, "missing_rows": 1}]
    gaps = await worker._detect_gaps(0, 900)
    assert gaps == worker._db.gaps


async def test_detect_gaps_swallows_db_errors():
    class ExplodingDb(FakeDb):
        async def get_unit_range(self, *a, **kw):
            raise RuntimeError("db down")

    w = _ConnectionWorker(_make_conn(), db=ExplodingDb(), source=FakeSource())
    gaps = await w._detect_gaps(0, 900)
    assert gaps == []


async def test_batch_is_complete_when_every_row_is_stored(worker):
    worker._db.stored_rows = 11
    assert await worker._batch_complete(0, 600) is True


async def test_gap_inside_stored_data_is_not_complete(worker):
    worker._db.unit_range = (0, 6000)
    worker._db.stored_rows = 8
    assert await worker._batch_complete(0, 600) is False


async def test_batch_complete_false_when_no_data(worker):
    worker._db.unit_range = (None, None)
    assert await worker._batch_complete(0, 800) is False


async def test_official_bar_replaces_the_streamed_one():
    source, db = FakeSource(), FakeDb()
    worker = _ConnectionWorker(_make_conn(), db=db, source=source, executor=FakeExecutor())
    assert await worker._store_official_bar(600) is True
    assert worker._executor.calls == [("BTC_USDT", 60, 600, 600)]
    assert len(db.inserted) == 1


async def test_a_missing_official_bar_is_reported_not_raised():
    class Failing(FakeSource):
        async def get_historical_data(self, *a, **kw):
            raise RuntimeError("down")

    worker = _ConnectionWorker(_make_conn(), db=FakeDb(), source=Failing(), executor=FakeExecutor())
    assert await worker._store_official_bar(600) is False


async def test_a_connection_without_a_start_date_collects_no_history():
    source = ScriptSource(code=(
        "class S(DataSource):\n"
        "    streams = {'news': Stream(table='news', history=False)}\n"
        "    async def history(self, *a): return []\n"
        "    async def live(self, stream, symbol):\n"
        "        return\n"
        "        yield {}\n"
    ))
    conn = {"name": "news", "source": "news.py", "config": "{}"}
    worker = _ConnectionWorker(conn, db=FakeDb(), source=source, executor=FakeExecutor())
    assert worker._symbol == "" and worker._start_date is None


def test_table_comes_from_the_stream():
    source = ScriptSource(code=(
        "class S(DataSource):\n"
        "    streams = {'funding': Stream(table='funding_rates', step=28800)}\n"
        "    async def history(self, *a): return []\n"
    ))
    w = _ConnectionWorker(_make_conn(), db=FakeDb(), source=source)
    assert w._table == "funding_rates"
    assert w._unit_interval == 28800
    assert w._timeframe == "8h"


def test_connection_config_selects_the_stream():
    source = ScriptSource(code=(
        "class S(DataSource):\n"
        "    streams = {\n"
        "        'candles': Stream(table='candles', step=60),\n"
        "        'funding': Stream(table='funding_rates', step=28800),\n"
        "    }\n"
        "    async def history(self, *a): return []\n"
    ))
    w = _ConnectionWorker(_make_conn(config='{"stream": "funding"}'), db=FakeDb(), source=source)
    assert w._table == "funding_rates"


def test_table_name_falls_back_to_exchange_prefix():
    conn = _make_conn(source="whitebit.py")
    source = ScriptSource(code="pass")  # no TABLE_NAME declared
    w = _ConnectionWorker(conn, db=FakeDb(), source=source)
    assert w._table == "unit_whitebit"


def test_per_connection_config_overrides_timing_defaults():
    conn = _make_conn(config='{"gap_interval_s": 5, "batch_size": 100}')
    w = _ConnectionWorker(conn, db=FakeDb(), source=FakeSource())
    assert w._gap_interval == 5
    assert w._batch_size == 100


def test_per_connection_config_overrides_backoff_defaults():
    conn = _make_conn(config='{"max_gap_interval_s": 30, "stalled_threshold": 2}')
    w = _ConnectionWorker(conn, db=FakeDb(), source=FakeSource())
    assert w._max_gap_interval == 30
    assert w._stalled_threshold == 2


# ------------------------------------------------------------------ #
# Gap-recovery backoff/give-up (TASK-011)                              #
# ------------------------------------------------------------------ #

def test_gap_interval_is_unbacked_off_with_no_failures(worker):
    assert worker._current_gap_interval() == worker._gap_interval


def test_gap_interval_backs_off_exponentially(worker):
    worker._gap_failures = 1
    assert worker._current_gap_interval() == worker._gap_interval * 2
    worker._gap_failures = 3
    assert worker._current_gap_interval() == worker._gap_interval * 8


def test_gap_interval_caps_at_max_gap_interval(worker):
    worker._gap_failures = 20  # would be an enormous multiple uncapped
    assert worker._current_gap_interval() == worker._max_gap_interval


async def test_gap_cycle_resets_failures_and_restores_status_on_progress(worker):
    worker.progress["status"] = "streaming"
    worker._gap_failures = 3
    worker._recover_gaps = _async_return(True)

    await worker._gap_cycle()

    assert worker._gap_failures == 0
    assert worker.progress["status"] == "streaming"


async def test_gap_cycle_increments_failures_and_restores_status_below_threshold(worker):
    worker.progress["status"] = "streaming"
    worker._stalled_threshold = 5
    worker._recover_gaps = _async_return(False)

    await worker._gap_cycle()

    assert worker._gap_failures == 1
    assert worker.progress["status"] == "streaming"  # not yet at threshold


async def test_gap_cycle_marks_stalled_at_threshold(worker):
    worker.progress["status"] = "streaming"
    worker._stalled_threshold = 2
    worker._gap_failures = 1
    worker._recover_gaps = _async_return(False)

    await worker._gap_cycle()

    assert worker._gap_failures == 2
    assert worker.progress["status"] == "stalled"


async def test_gap_cycle_stays_stalled_across_repeated_failures():
    """Regression: capturing "prev_status" naively on every cycle would let a stalled
    connection's own "stalled" status get recorded as the thing to restore to -- it
    should keep restoring to the real underlying status (streaming) from before the
    first failure, not to "stalled" itself."""
    w = _ConnectionWorker(_make_conn(), db=FakeDb(), source=FakeSource())
    w.progress["status"] = "streaming"
    w._stalled_threshold = 1
    w._recover_gaps = _async_return(False)

    await w._gap_cycle()  # 1st failure -> stalled
    assert w.progress["status"] == "stalled"
    await w._gap_cycle()  # 2nd failure while already stalled -> still stalled
    assert w.progress["status"] == "stalled"
    assert w._gap_failures == 2

    w._recover_gaps = _async_return(True)  # source recovers
    await w._gap_cycle()

    assert w._gap_failures == 0
    assert w.progress["status"] == "streaming"  # restored to the real prior status


# ------------------------------------------------------------------ #
# DataCollector — connection lifecycle with a fully fake db            #
# ------------------------------------------------------------------ #

class FakeCollectorDb:
    def __init__(self):
        self.connections: dict[str, dict] = {}
        self.ensured = False

    async def ensure_connections_table(self):
        self.ensured = True

    async def list_connections(self):
        return list(self.connections.values())

    async def upsert_connection(self, conn):
        self.connections[conn["name"]] = conn

    async def delete_connection(self, name):
        self.connections.pop(name, None)

    async def get_plugin_code(self, project, type_, name):
        return None  # simulate "script not found" -- exercises the early-return path


async def test_data_collector_start_with_no_connections():
    db = FakeCollectorDb()
    collector = DataCollector(db=db)
    await collector.start()
    assert db.ensured is True
    assert collector.get_status() == {}


async def test_data_collector_add_connection_missing_script_does_not_crash():
    db = FakeCollectorDb()
    collector = DataCollector(db=db)
    await collector.add_connection(_make_conn())
    assert "wb_btc_1m" in db.connections
    # get_plugin_code returns None -> worker never registered, but must not raise
    assert collector.get_status() == {}


async def test_data_collector_remove_connection():
    db = FakeCollectorDb()
    collector = DataCollector(db=db)
    await collector.add_connection(_make_conn())
    await collector.remove_connection("wb_btc_1m")
    assert "wb_btc_1m" not in db.connections


async def test_data_collector_context_manager_starts_and_stops():
    db = FakeCollectorDb()
    async with DataCollector(db=db):
        assert db.ensured is True
    # __aexit__ must not raise even with zero active workers

async def test_a_source_cannot_write_to_a_table_the_host_forbids():
    db = FakeDb()
    worker = _ConnectionWorker(
        _make_conn(), db=db, source=FakeSource(), executor=FakeExecutor(),
        table_allowed=lambda table: table.startswith("data_"),
    )
    stored = await worker._fetch_and_store(0, 600, 1, 1)
    assert stored == 0 and db.inserted == []


async def test_history_that_fails_outright_is_an_error_not_live():
    """An unknown symbol fails every history request, and its live stream may connect and
    then stay silent: the connection reported "streaming" with nothing in it."""
    class FailingSession(SourceSession):
        async def fetch(self, *args, **kwargs):
            raise ValueError("Binance: Invalid symbol. (SOL_USTD)")

    class FailingExecutor(FakeExecutor):
        async def open_source(self, source):
            return FailingSession(source)

    worker = _ConnectionWorker(_make_conn(start_date="2026-01-01"), db=FakeDb(),
                               source=FakeSource(), executor=FailingExecutor())
    worker._safe_end = lambda: worker._parse_start_ts() + 1800
    await worker._backfill()
    assert worker.progress["status"] == "error"
    assert "Invalid symbol" in worker.progress["error"]


async def test_a_connection_keeps_only_the_days_it_is_told_to():
    import time as _time

    class PruneDb(FakeDb):
        def __init__(self):
            super().__init__()
            self.deleted = []

        async def delete_unit_rows_before(self, table, exchange, symbol, before_ts=None):
            self.deleted.append((table, exchange, symbol, before_ts))

    db = PruneDb()
    worker = _make_worker(db=db, start_date="2020-01-01T00:00:00Z", config='{"keep_days": 2}')
    two_days_ago = _time.time() - 2 * 86400
    assert abs(worker._parse_start_ts() - two_days_ago) <= 60, "history starts at what it keeps"
    await worker._prune()
    await worker._prune()
    assert len(db.deleted) == 1, "at most once per prune interval"
    _, exchange, symbol, before = db.deleted[0]
    assert (exchange, symbol) == ("whitebit", "BTC_USDT") and abs(before - two_days_ago) <= 60


async def test_the_first_prune_does_not_wait_on_a_freshly_booted_machine(monkeypatch):
    """monotonic() counts from boot: on a CI runner up for minutes, a start of 0 read as a
    prune moments ago and the first one was skipped."""
    import time as _time

    class PruneDb(FakeDb):
        def __init__(self):
            super().__init__()
            self.deleted = []

        async def delete_unit_rows_before(self, table, exchange, symbol, before_ts=None):
            self.deleted.append(before_ts)

    monkeypatch.setattr(_time, "monotonic", lambda: 120.0)
    db = PruneDb()
    worker = _make_worker(db=db, start_date="2020-01-01T00:00:00Z", config='{"keep_days": 2}')
    await worker._prune()
    assert len(db.deleted) == 1


async def test_without_a_limit_everything_is_kept():
    class PruneDb(FakeDb):
        async def delete_unit_rows_before(self, *args, **kwargs):
            raise AssertionError("nothing is deleted")

    worker = _make_worker(db=PruneDb(), start_date="2020-01-01T00:00:00Z")
    await worker._prune()
    assert worker._parse_start_ts() == 1577836800
