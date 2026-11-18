"""AggregationWorker — cross-source scripts run in a worker; the host only sees plain data."""
from __future__ import annotations

import polars as pl
import pytest

from tradingkit.aggregation import (
    AggregationWorker,
    describe_cross_source,
    output_table_name,
    output_table_prefix,
)

SQL_SCRIPT = '''
OUTPUT_TABLE = "spread"
SOURCES = {
    "btc": SourceRef("candles", field="close", filters={"exchange": "wb", "symbol": "BTC_USDT", "timeframe": "1m"}),
    "eth": SourceRef("candles", field="close", filters={"exchange": "wb", "symbol": "ETH_USDT", "timeframe": "1m"}),
}
COMBINE_SQL = "btc - eth"
'''

COMBINE_SCRIPT = '''
OUTPUT_TABLE = "ratio"
SOURCE_FILTERS = {"exchange": "wb", "timeframe": "1m"}

async def combine(ctx, start_ts, end_ts):
    btc = await ctx.query("candles", symbol="BTC_USDT", start_ts=start_ts, end_ts=end_ts)
    return [{"timestamp": t, "value": c * 2} for t, c in zip(btc["timestamp"], btc["close"])]
'''


class FakeDb:
    _conn = object()

    def __init__(self, scripts, mvs=()):
        self.scripts = list(scripts)
        self.mvs = list(mvs)
        self.sql: list[str] = []
        self.reads: list = []
        self.inserted: list = []
        self.setups: list = []
        self.rows = pl.DataFrame({"timestamp": [60, 120], "close": [10.0, 11.0]})

    async def _execute(self, sql, params=None, **kw):
        self.sql.append(sql)
        if "FROM plugin_library" in sql:
            return self.scripts
        if "system.tables" in sql:
            return [(name,) for name in self.mvs]
        return []

    async def fetch_from_unit_table(self, table, symbol=None, start_ts=None, end_ts=None, filters=None):
        self.reads.append((table, symbol, filters))
        return self.rows

    async def get_table_schema(self, table):
        raise LookupError(table)

    async def assert_source_unambiguous(self, table, filters):
        pass

    async def ensure_cross_source_table(self, table, columns):
        self.setups.append(("table", table, sorted(columns)))

    async def ensure_cross_source_mv(self, output, source, alias, field, ch_type, filters=None):
        self.setups.append(("mv", f"mv_{source}_to_{output}_{alias}"))

    async def backfill_cross_source(self, output, source, alias, field, ch_type, filters=None):
        self.setups.append(("backfill", alias))

    async def ensure_raw_table(self, table, schema):
        pass

    async def insert_unit_batch(self, table, df, exchange, symbol, timeframe):
        self.inserted.append((table, df, exchange))

    async def flush(self):
        pass


def test_describe_returns_the_declarations_as_plain_data():
    described = describe_cross_source(SQL_SCRIPT)
    assert described["output_table"] == "spread"
    assert described["combine_sql"] == "btc - eth"
    assert described["sources"]["btc"] == {
        "table": "candles", "field": "close", "ch_type": "Float64",
        "filters": {"exchange": "wb", "symbol": "BTC_USDT", "timeframe": "1m"},
    }
    assert described["has_combine"] is False


def test_output_tables_belong_to_their_project():
    assert output_table_name("alice", "spread") != output_table_name("bob", "spread")
    assert output_table_name("a-b", "spread") != output_table_name("a_b", "spread")
    assert output_table_name("alice", "users").startswith("agg_alice_")
    for bad in ("Users", "1x", "x;drop", "", "a" * 49):
        with pytest.raises(ValueError, match="OUTPUT_TABLE"):
            output_table_name("alice", bad)


async def test_combine_sql_is_installed_once_per_version_of_the_code():
    db = FakeDb([("alice", "spread", SQL_SCRIPT)])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    await worker.run_once()
    table = output_table_name("alice", "spread")
    assert db.setups.count(("table", table, ["btc", "eth"])) == 1
    assert ("mv", f"mv_candles_to_{table}_btc") in db.setups
    assert worker.status[("alice", "spread")] is None


async def test_mvs_of_a_removed_script_are_dropped():
    table = output_table_name("alice", "spread")
    db = FakeDb([], mvs=[f"mv_candles_to_{table}_btc"])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    drops = [s for s in db.sql if s.startswith("DROP VIEW")]
    assert drops == [f"DROP VIEW IF EXISTS mv_candles_to_{table}_btc"]
    listing, = [s for s in db.sql if "system.tables" in s]
    assert "position(name, '_to_agg_') > 0" in listing


async def test_mvs_of_a_live_script_are_kept():
    table = output_table_name("alice", "spread")
    mvs = [f"mv_candles_to_{table}_btc", f"mv_candles_to_{table}_eth"]
    db = FakeDb([("alice", "spread", SQL_SCRIPT)], mvs=mvs)
    await AggregationWorker(db, table_allowed=lambda t: t == "candles").run_once()
    assert not [s for s in db.sql if s.startswith("DROP VIEW")]


async def test_a_source_outside_the_allowed_tables_is_refused():
    script = SQL_SCRIPT.replace('SourceRef("candles"', 'SourceRef("users"', 1)
    db = FakeDb([("alice", "spread", script)])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    assert "may not read" in worker.status[("alice", "spread")]
    assert db.setups == []


async def test_combine_runs_through_the_executor_and_writes_the_project_tag():
    db = FakeDb([("alice", "ratio", COMBINE_SCRIPT)])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    assert worker.status[("alice", "ratio")] is None
    (table, df, tag), = db.inserted
    assert table == output_table_name("alice", "ratio") and tag == "alice"
    assert df["value"].to_list() == [20.0, 22.0]
    assert db.reads == [("candles", "BTC_USDT", {"exchange": "wb", "timeframe": "1m"})]


async def test_ctx_query_cannot_read_other_tables():
    script = COMBINE_SCRIPT.replace('ctx.query("candles"', 'ctx.query("users"')
    db = FakeDb([("alice", "ratio", script)])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    assert "may not read table 'users'" in worker.status[("alice", "ratio")]
    assert db.reads == [] and db.inserted == []


async def test_a_script_with_nothing_to_run_reports_it():
    db = FakeDb([("alice", "empty", 'OUTPUT_TABLE = "empty"\n')])
    worker = AggregationWorker(db)
    await worker.run_once()
    assert "neither COMBINE_SQL nor combine()" in worker.status[("alice", "empty")]


async def test_dropping_a_project_removes_its_views_before_its_tables():
    table = output_table_name("alice", "spread")
    other = output_table_name("bob", "spread")
    db = FakeDb([], mvs=[f"mv_candles_to_{table}_btc", f"mv_candles_to_{other}_btc"])
    tables = [(table,)]

    async def execute(sql, params=None, **kw):
        db.sql.append(sql)
        if "engine = 'MaterializedView'" in sql:
            return [(name,) for name in db.mvs]
        if "startsWith(name, %(prefix)s)" in sql:
            assert params["prefix"] == output_table_prefix("alice")
            return tables
        return []

    db._execute = execute
    await AggregationWorker(db).drop_project("alice")
    drops = [s for s in db.sql if s.startswith("DROP")]
    assert drops == [f"DROP VIEW IF EXISTS mv_candles_to_{table}_btc", f"DROP TABLE IF EXISTS {table}"]
