"""AggregationWorker — cross-source scripts run in a worker; the host only sees plain data."""
from __future__ import annotations

import functools
import json

import polars as pl
import pytest

from tradingkit.aggregation import (
    INSTANCE_TYPE,
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


TEMPLATE = '''
INPUTS = ["a", "b"]

async def combine(ctx, start_ts, end_ts):
    a = await ctx.input("a", start_ts, end_ts)
    b = await ctx.input("b", start_ts, end_ts)
    return [{"timestamp": t, "value": x / y} for t, x, y in zip(a["timestamp"], a["close"], b["close"])]
'''

SQL_TEMPLATE = '''
INPUTS = ["a", "b"]
COMBINE_SQL = "a - b"
'''

SOURCE = (
    "class S(DataSource):\n"
    "    streams = {'candles': Stream(table='candles', step=60)}\n"
)

CONNECTIONS = [
    {"name": "eth", "project": "alice", "source": "wb.py", "symbol": "ETH_USDT", "timeframe": "1m", "config": "{}"},
    {"name": "btc", "project": "alice", "source": "wb.py", "symbol": "BTC_USDT", "timeframe": "1m", "config": "{}"},
    {"name": "bobs", "project": "bob", "source": "wb.py", "symbol": "SOL_USDT", "timeframe": "1m", "config": "{}"},
]


def run_row(project, name, script, inputs):
    return (project, INSTANCE_TYPE, name, json.dumps({"script": script, "inputs": inputs}))


class FakeDb:
    _conn = object()

    def __init__(self, scripts, mvs=()):
        # (namespace, name, code) rows are scripts; four-tuples carry their own type.
        self.scripts = [row if len(row) == 4 else (row[0], "aggregation", *row[1:]) for row in scripts]
        self.mvs = list(mvs)
        self.sql: list[str] = []
        self.reads: list = []
        self.warmups: list = []
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

    async def list_connections(self):
        return CONNECTIONS

    async def get_plugin_code(self, namespace, type_, name):
        return SOURCE if (type_, name) == ("source", "wb.py") else None

    async def fetch_from_unit_table(self, table, symbol=None, start_ts=None, end_ts=None, filters=None,
                                    limit=None, newest_first=False):
        if limit is not None:
            self.warmups.append((table, end_ts, limit))
            return pl.DataFrame({"timestamp": [0], "close": [9.0]})
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


def test_describe_lists_the_roles():
    assert describe_cross_source(TEMPLATE)["inputs"] == ["a", "b"]
    with pytest.raises(ValueError, match="INPUTS role"):
        describe_cross_source('INPUTS = ["A b"]\n')


async def test_a_template_does_not_run_by_itself():
    db = FakeDb([("alice", "pair.py", TEMPLATE)])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    assert db.inserted == [] and worker.status == {}


async def test_a_run_reads_the_bound_connections():
    db = FakeDb([("alice", "pair.py", TEMPLATE), run_row("alice", "eth_btc", "pair.py", {"a": "eth", "b": "btc"})])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    assert worker.status[("alice", "eth_btc")] is None
    assert db.reads == [
        ("candles", None, {"exchange": "wb", "symbol": "ETH_USDT", "timeframe": "1m"}),
        ("candles", None, {"exchange": "wb", "symbol": "BTC_USDT", "timeframe": "1m"}),
    ]
    (table, df, tag), = db.inserted
    assert table == output_table_name("alice", "eth_btc") and tag == "alice"
    assert df["value"].to_list() == [1.0, 1.0]


async def test_combine_sql_runs_over_the_bound_connections():
    db = FakeDb([("alice", "spread.py", SQL_TEMPLATE), run_row("alice", "eth_btc", "spread.py", {"a": "eth", "b": "btc"})])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    table = output_table_name("alice", "eth_btc")
    assert ("table", table, ["a", "b"]) in db.setups
    assert ("mv", f"mv_candles_to_{table}_a") in db.setups


async def test_a_run_needs_every_role_and_only_its_own_connections():
    db = FakeDb([
        ("alice", "pair.py", TEMPLATE),
        run_row("alice", "half", "pair.py", {"a": "eth"}),
        run_row("alice", "foreign", "pair.py", {"a": "eth", "b": "bobs"}),
        run_row("alice", "orphan", "gone.py", {"a": "eth", "b": "btc"}),
    ])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    assert "Choose a connection for b" in worker.status[("alice", "half")]
    assert "No connection 'bobs'" in worker.status[("alice", "foreign")]
    assert "is gone" in worker.status[("alice", "orphan")]
    assert db.inserted == []


async def test_ctx_input_names_only_declared_roles():
    script = TEMPLATE.replace('ctx.input("b"', 'ctx.input("c"')
    db = FakeDb([("alice", "pair.py", script), run_row("alice", "eth_btc", "pair.py", {"a": "eth", "b": "btc"})])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    assert "No input 'c'" in worker.status[("alice", "eth_btc")]


async def test_dropping_an_output_removes_its_table():
    db = FakeDb([])
    worker = AggregationWorker(db)
    worker.status[("alice", "eth_btc")] = "old"
    await worker.drop_output("alice", "eth_btc")
    assert f"DROP TABLE IF EXISTS {output_table_name('alice', 'eth_btc')}" in db.sql
    assert ("alice", "eth_btc") not in worker.status


async def test_a_run_sets_the_script_parameters():
    script = TEMPLATE.replace('INPUTS = ["a", "b"]', 'INPUTS = ["a", "b"]\n__params__ = {"scale": {"type": "float", "default": 1.0}}') \
        .replace('"value": x / y', '"value": x / y * scale')
    db = FakeDb([
        ("alice", "pair.py", script),
        (run_row("alice", "scaled", "pair.py", {"a": "eth", "b": "btc"})[:3]
         + (json.dumps({"script": "pair.py", "inputs": {"a": "eth", "b": "btc"}, "params": {"scale": "3"}}),)),
        (run_row("alice", "bad", "pair.py", {"a": "eth", "b": "btc"})[:3]
         + (json.dumps({"script": "pair.py", "inputs": {"a": "eth", "b": "btc"}, "params": {"nope": 1}}),)),
    ])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    await worker.run_once()
    (table, df, _), = db.inserted
    assert table == output_table_name("alice", "scaled") and df["value"].to_list() == [3.0, 3.0]
    assert "no parameter nope" in worker.status[("alice", "bad")]


async def test_ctx_input_adds_warmup_rows_before_the_range():
    script = (TEMPLATE.replace('ctx.input("a", start_ts, end_ts)', 'ctx.input("a", start_ts, end_ts, warmup=5)')
              .replace('ctx.input("b", start_ts, end_ts)', 'ctx.input("b", start_ts, end_ts, warmup=5)'))
    db = FakeDb([("alice", "pair.py", script)])
    worker = AggregationWorker(db, table_allowed=lambda t: t == "candles")
    agg = await worker.describe_run("alice", "eth_btc", script, {"a": "eth", "b": "btc"})
    rows = await worker._executor.call(
        "tradingkit.aggregation:run_cross_source_combine",
        {"code": script, "start_ts": 60, "end_ts": 200, "source_filters": {}},
        on_request=functools.partial(worker.serve, inputs=agg.bound_inputs),
    )
    assert [r["timestamp"] for r in rows] == [0, 60, 120]
    assert db.warmups == [("candles", 59, 5), ("candles", 59, 5)]
