"""
tradingkit.aggregation — AggregationContext, Aggregation, ScriptAggregation, AggregationWorker.

AggregationContext  — project-level object passed to Aggregation.combine(ctx, start_ts, end_ts).
                      Provides query() access to ANY table in ClickHouse.

Aggregation         — declarative cross-source aggregation (like Strategy declares
                      Indicators via class attributes). Declare SourceRef class attributes,
                      then either:
                        combine_sql() -> str   — SQL expression over the aliases, computed
                                                  entirely in ClickHouse via a gap-closing
                                                  Materialized View (see setup_aggregation) —
                                                  no polling, no Python roundtrip.
                        combine(ctx, start_ts, end_ts) -> list[dict] — Python fallback for
                                                  logic SQL can't express (windows, joins
                                                  across more than the declared sources,
                                                  external state). Same ctx.query() access as
                                                  the old aggregate(ctx, ...) free-function
                                                  convention this replaces — a strict
                                                  superset, not a narrower callback.
                      A class defining combine_sql() needs no worker at all: setup_aggregation()
                      installs the MV once and ClickHouse keeps it live forever. A class using
                      only combine() is driven by AggregationWorker on a timer, same as before.

ScriptAggregation   — wraps a code string (from plugin_library, SaaS UI editor) the same way
                      ScriptStrategy/ScriptIndicator wrap user code — dynamic equivalent of a
                      hand-written Aggregation subclass, loaded via load_aggregation_plugin().

AggregationWorker   — background task that periodically drives Aggregation/ScriptAggregation
                      instances whose combine_sql() is None (the ones combine_sql() covers
                      need it exactly zero times — ClickHouse's own MV mechanism keeps them
                      current on every insert).

AggregationScript   — NOT the same thing as Aggregation above, despite the name: this is the
                      *within-table* bucket-aggregation script wrapper (`aggregation(unit) ->
                      AggSpec`, consumed by DataCollector to auto-create per-connection
                      candles_60s/_3600s Materialized Views — tradingkit/schema.py's Fold
                      machinery). Cross-table aggregate(ctx, start_ts, end_ts) support has
                      been removed from this class in favor of Aggregation/ScriptAggregation
                      above; the within-table half is unrelated and unchanged.
"""
from __future__ import annotations

import asyncio
import functools
import hashlib
import io
import json
import logging
import re
import time
from abc import ABC
from collections.abc import Callable
from typing import Any, ClassVar

import polars as pl

from tradingkit.core.clickhouse import ClickHouseManager

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 60  # seconds between AggregationWorker runs

#: plugin_library type of a run: a script with INPUTS bound to connections, stored as
#: {"script": <file name>, "inputs": {role: connection name}} under the run's name.
INSTANCE_TYPE = "aggregation_instance"


# ---------------------------------------------------------------------------
# AggregationContext
# ---------------------------------------------------------------------------

class AggregationContext:
    """
    Project-level query context passed to Aggregation.combine(ctx, start_ts, end_ts).

    Provides access to ALL tables in ClickHouse via query().
    Scripts should use this to fetch any raw or pre-aggregated table.

    Example:
        btc = await ctx.query("candles", symbol="BTC_USDT", start_ts=start_ts, end_ts=end_ts)
        eth = await ctx.query("candles", symbol="ETH_USDT", start_ts=start_ts, end_ts=end_ts)
        joined = btc.join(eth, on="timestamp", suffix="_eth")
    """

    def __init__(
        self, db: ClickHouseManager, source_filters: dict | None = None,
        inputs: dict[str, tuple[str, dict]] | None = None,
    ) -> None:
        self._db = db
        #: role -> (table, filters) of the connection bound to it, see Aggregation.INPUTS.
        self._inputs = dict(inputs or {})
        #: Default equality filters applied to every query() — declared by the aggregation
        #: (Aggregation.SOURCE_FILTERS), not inferred. This used to be a single `exchange`
        #: string that the worker filled in from the *project namespace*, so a project
        #: named "my_bot" reading data tagged "whitebit" matched zero rows and combine()
        #: returned nothing, silently.
        self._source_filters = dict(source_filters or {})

    async def query(
        self,
        table: str,
        symbol: str | None = None,
        start_ts: int | None = None,
        end_ts:   int | None = None,
        **filters: Any,
    ) -> pl.DataFrame:
        """
        Fetch from any ClickHouse table with optional identity and time filters.

        Keyword filters narrow further for this one call and override the aggregation's
        SOURCE_FILTERS, e.g. ctx.query("candles", exchange="binance").

        For raw unit tables (ReplacingMergeTree): plain SELECT *.
        For aggregating tables, the script is responsible for using the right table name
        (e.g. 'candles_3600s' for pre-computed 1h buckets).
        """
        merged = {**self._source_filters, **filters}
        return await self._db.fetch_from_unit_table(
            table, symbol=symbol, start_ts=start_ts, end_ts=end_ts, filters=merged
        )

    async def input(
        self, role: str, start_ts: int | None = None, end_ts: int | None = None, warmup: int = 0,
    ) -> pl.DataFrame:
        """Rows of the connection bound to `role`, one of the aggregation's INPUTS.
        `warmup` adds that many rows from before start_ts, for rolling windows."""
        if role not in self._inputs:
            raise KeyError(f"No input {role!r}; this run binds {sorted(self._inputs)}")
        table, filters = self._inputs[role]
        return await read_input(self._db, table, filters, start_ts, end_ts, warmup)


# ---------------------------------------------------------------------------
# SourceRef + Aggregation — declarative cross-source aggregation
# ---------------------------------------------------------------------------

class SourceRef:
    """
    Declares one source a cross-source Aggregation reads from — a ClickHouse table, which
    column to combine, and which rows of that table belong to this source.

        # one table per series
        btc = SourceRef("candles_btc_usdt", field="close")

        # one shared table holding many series -- what a real collector writes
        btc = SourceRef("candles", field="close",
                        filters={"exchange": "whitebit", "symbol": "BTC_USDT"})

    `filters` is equality-only and is rendered into the source's Materialized View. Without
    it on a shared table the MV aggregates every row, and argMax picks whichever series won
    at each timestamp — one column silently interleaving BTC, ETH and SOL. setup_aggregation()
    refuses to build such an aggregation rather than let it return plausible garbage.
    """

    def __init__(
        self,
        table: str,
        field: str = "close",
        ch_type: str = "Float64",
        filters: dict | None = None,
    ) -> None:
        self.table   = table
        self.field   = field
        self.ch_type = ch_type
        self.filters = dict(filters or {})


class Aggregation(ABC):
    """
    Declarative cross-source aggregation. Subclass and declare SourceRef class attributes
    (the same pattern Strategy uses for Indicator declarations), then implement either
    combine_sql() or combine() — see the module docstring for the tradeoff between them.

        class BtcEthSpread(Aggregation):
            OUTPUT_TABLE = "btc_eth_spread"
            btc = SourceRef("candles", field="close", filters={"symbol": "BTC_USDT"})
            eth = SourceRef("candles", field="close", filters={"symbol": "ETH_USDT"})

            def combine_sql(self) -> str:
                return "btc - eth"
    """

    OUTPUT_TABLE: str

    #: Equality filters applied to every ctx.query() on the combine() path — the
    #: aggregation stating which data it reads, e.g. {"exchange": "whitebit"}. The worker
    #: used to substitute the *project namespace* here, so an aggregation in project
    #: "my_bot" filtered source rows by exchange="my_bot" and found none, silently. Only
    #: the combine() path needs this; combine_sql() sources carry their own SourceRef
    #: filters instead.
    SOURCE_FILTERS: ClassVar[dict] = {}

    #: Roles the aggregation reads, e.g. ["a", "b"], instead of naming markets in code.
    #: A run binds each role to a connection of the project, so one script serves any
    #: pair of markets; combine() reads a role with ctx.input("a", start_ts, end_ts), and
    #: COMBINE_SQL names the roles as columns (the bound rows' INPUT_FIELDS, default close).
    INPUTS: ClassVar[list[str]] = []
    INPUT_FIELDS: ClassVar[dict[str, str]] = {}

    def inputs(self) -> list[str]:
        return list(self.INPUTS)

    @classmethod
    def required_sources(cls) -> dict[str, SourceRef]:
        """Auto-collect all SourceRef class attributes — same __mro__/vars() scan as
        Strategy.required_indicators()."""
        result: dict[str, SourceRef] = {}
        for klass in reversed(cls.__mro__):
            for k, v in vars(klass).items():
                if isinstance(v, SourceRef):
                    result[k] = v
        return result

    def combine_sql(self) -> str | None:
        """
        SQL expression over the declared aliases (e.g. "btc - eth"), computed entirely in
        ClickHouse — no Python roundtrip. None (default) means there's no SQL-native path;
        use combine() instead.
        """
        return None

    def output_schema(self) -> dict[str, str] | None:
        """Optional explicit {column: ClickHouse type} for combine()'s output rows, cast
        before insertion. None (default) lets Polars infer types from the returned dicts."""
        return None

    async def combine(self, ctx: AggregationContext, start_ts: int, end_ts: int) -> list[dict] | None:
        """
        Python fallback for logic combine_sql() can't express — deliberately the same
        signature as the aggregate(ctx, start_ts, end_ts) free-function convention this
        replaces (full ctx.query() access, not a single merged row), so nothing that used
        to be possible stops being possible. Only called when combine_sql() returns None;
        driven by AggregationWorker on a timer rather than reactively.
        """
        return None


# ---------------------------------------------------------------------------
# ScriptAggregation — dynamic plugin, mirrors ScriptStrategy/ScriptIndicator
# ---------------------------------------------------------------------------

class ScriptAggregation(Aggregation):
    """
    Wraps a code string (from plugin_library / a UI editor) the same way ScriptStrategy
    wraps strategy code — the dynamic equivalent of hand-writing an Aggregation subclass,
    with no framework release needed to create a new one.

        OUTPUT_TABLE = "btc_eth_spread"
        SOURCES = {
            "btc": SourceRef("candles_btc_usdt", field="close"),
            "eth": SourceRef("candles_eth_usdt", field="close"),
        }
        COMBINE_SQL = "btc - eth"
    """

    def __init__(self, code: str, config: dict | None = None, params: dict | None = None) -> None:
        self._code = code
        self.config = config or {}
        #: A run's values for the script's __params__; defaults fill the rest.
        self.params = dict(params or {})
        self._exec_ns()

    def _exec_ns(self) -> None:
        # Same pattern as ScriptStrategy._exec_ns() — execute once, read from self._ns
        # afterward. Unlike ScriptStrategy, no NameError/AttributeError swallowing here:
        # a valid aggregation script has no forward references to values injected later
        # (SourceRef is injected up front, not deferred), so a failure here means the
        # script itself is broken and should fail loudly.
        from tradingkit.core.script_ast import resolve_params, script_params
        declared = script_params(self._code)
        values = {k: v for k, v in resolve_params(self._code, self.params).items() if k in declared}
        self._ns: dict[str, Any] = {"SourceRef": SourceRef, **values}
        exec(compile(self._code, "<aggregation_script>", "exec"), self._ns)  # noqa: S102

    def __getstate__(self) -> dict:
        # Same __builtins__-pickling fix as ScriptStrategy.__getstate__ — _ns is a derived
        # cache of self._code, rebuilt in __setstate__ instead of pickled.
        state = self.__dict__.copy()
        state.pop("_ns", None)
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._exec_ns()

    @property
    def OUTPUT_TABLE(self) -> str:  # matches the base class's class-attribute name
        return self._ns["OUTPUT_TABLE"]

    def required_sources(self) -> dict[str, SourceRef]:
        return self._ns.get("SOURCES", {})

    def combine_sql(self) -> str | None:
        return self._ns.get("COMBINE_SQL")

    def inputs(self) -> list[str]:
        declared = self._ns.get("INPUTS") or []
        return [str(role) for role in (declared.keys() if isinstance(declared, dict) else declared)]

    def output_schema(self) -> dict[str, str] | None:
        return self._ns.get("OUTPUT_SCHEMA")

    async def combine(self, ctx: AggregationContext, start_ts: int, end_ts: int) -> list[dict] | None:
        fn = self._ns.get("combine")
        if not callable(fn):
            return None
        result = fn(ctx, start_ts, end_ts)
        if asyncio.iscoroutine(result):
            result = await result
        return result


def load_aggregation_plugin(type_: str, params_dict: dict) -> Aggregation:
    """
    Create an Aggregation plugin from a type string and params dict — same dispatch shape
    as load_strategy_plugin/load_indicator_plugin.

    '__script__'  — ScriptAggregation wrapping user code (from '_code' param).
    named types   — looks up in a host-registered builtin registry, see
                    tradingkit.core.plugin_registry.
    """
    if type_ == "__script__":
        code = params_dict.get("_code", "")
        if not code:
            raise ValueError("Aggregation type '__script__' requires '_code' in params")
        return ScriptAggregation(code=code, config=params_dict)

    from tradingkit.core.plugin_registry import get_builtin_registry
    builtins_ = get_builtin_registry("TRADINGKIT_AGGREGATIONS_MODULE", "BUILTIN_AGGREGATIONS")
    code = builtins_.get(type_)

    if not code:
        raise ValueError(
            f"Unknown aggregation type {type_!r}. "
            f"Use '__script__' with '_code', or a registered builtin name."
        )
    return ScriptAggregation(code=code, config=params_dict)


# ---------------------------------------------------------------------------
# setup_aggregation / query_aggregation — orchestration for Aggregation instances
# ---------------------------------------------------------------------------

async def setup_aggregation(db: ClickHouseManager, agg: Aggregation) -> None:
    """
    One-time idempotent setup for an Aggregation instance.

    combine_sql() defined  — installs the gap-closing alignment table + one Materialized
                             View per declared source (ensure_cross_source_table/
                             ensure_cross_source_mv), then backfills each source's existing
                             rows (backfill_cross_source) — MVs are not retroactive, they
                             only fire on rows inserted after the MV itself exists, so
                             setting this up on top of already-populated source tables
                             would otherwise silently have no historical data. ClickHouse
                             keeps the result live from here on; call this once (e.g. at
                             app startup, or when a new aggregation is created via the SaaS
                             UI), never poll again.
    combine_sql() is None  — just ensures OUTPUT_TABLE exists for AggregationWorker to
                             write into; the actual periodic computation happens there.
    """
    sources = agg.required_sources()
    sql = agg.combine_sql()

    if sql is not None:
        # Refuse to build an aggregation whose sources are ambiguous, before any DDL runs:
        # a shared table with no filter yields a column that interleaves several series,
        # and nothing downstream can tell that apart from a real one.
        for ref in sources.values():
            await db.assert_source_unambiguous(ref.table, ref.filters)

        await db.ensure_cross_source_table(
            agg.OUTPUT_TABLE, {alias: ref.ch_type for alias, ref in sources.items()}
        )
        for alias, ref in sources.items():
            # The MV covers rows from now on, the backfill covers rows already there --
            # both must get the same filters or history and live data disagree.
            await db.ensure_cross_source_mv(
                agg.OUTPUT_TABLE, ref.table, alias, ref.field, ref.ch_type,
                filters=ref.filters,
            )
            await db.backfill_cross_source(
                agg.OUTPUT_TABLE, ref.table, alias, ref.field, ref.ch_type,
                filters=ref.filters,
            )
        return

    schema = agg.output_schema()
    if schema:
        _PL_MAP = {
            "Int64":   pl.Int64,   "UInt64":  pl.UInt64,
            "Float64": pl.Float64, "Float32": pl.Float32,
            "String":  pl.String,
        }
        pl_schema = {k: _PL_MAP.get(v, pl.String) for k, v in schema.items()}
        await db.ensure_raw_table(agg.OUTPUT_TABLE, pl_schema)
    # Without an explicit output_schema(), OUTPUT_TABLE gets created lazily on the first
    # real combine() result — same as AggregationWorker._run_one() does today.


async def query_aggregation(
    db: ClickHouseManager,
    agg: Aggregation,
    start_ts: int,
    end_ts: int,
    exchange: str = "",
) -> list[dict]:
    """
    Read an Aggregation's result — the single entry point regardless of which path
    computed it.

    combine_sql() defined — query_cross_source() against the alignment table, computed at
                            read time, always fresh, only_complete=True (drops rows where a
                            source hasn't arrived yet rather than surfacing a misleading
                            NULL/0 unannounced).
    combine_sql() is None — plain AggregationContext.query() against OUTPUT_TABLE, i.e.
                            whatever AggregationWorker last stored (fresh to within
                            _POLL_INTERVAL, same staleness bound as before).
    """
    sql = agg.combine_sql()
    if sql is not None:
        aliases = list(agg.required_sources())
        return await db.query_cross_source(agg.OUTPUT_TABLE, aliases, sql, start_ts, end_ts)

    # `exchange` here selects rows by the tag AggregationWorker stamped on its own output,
    # not by a source exchange. Empty means "any", matching fetch_from_unit_table's now
    # optional filter -- it used to filter for exchange = '' and so matched nothing unless
    # the caller happened to know the worker's tag.
    filters = {"exchange": exchange} if exchange else {}
    df = await AggregationContext(db, filters).query(
        agg.OUTPUT_TABLE, start_ts=start_ts, end_ts=end_ts
    )
    return df.to_dicts()


# ---------------------------------------------------------------------------
# Cross-source scripts in a worker: the host reads them as plain data
# ---------------------------------------------------------------------------

_OUTPUT_NAME_RE = re.compile(r"[a-z][a-z0-9_]{0,47}")
_PL_TYPES = {
    "Int64": pl.Int64, "UInt64": pl.UInt64, "Float64": pl.Float64, "Float32": pl.Float32,
    "String": pl.String,
}


def output_table_prefix(namespace: str) -> str:
    """The start of every output table of one project, and of no other project's."""
    slug = re.sub(r"[^a-z0-9]+", "_", namespace.lower()).strip("_")[:24] or "default"
    tag = hashlib.sha256(namespace.encode()).hexdigest()[:6]
    return f"agg_{slug}_{tag}_"


def output_table_name(namespace: str, output_table: str) -> str:
    """The table a project's aggregation writes to.

    Derived from the project as well as OUTPUT_TABLE, so two projects never share an
    output, and a script can only ever name a table starting with `agg_`.
    """
    if not _OUTPUT_NAME_RE.fullmatch(output_table or ""):
        raise ValueError(
            f"OUTPUT_TABLE {output_table!r}: use lowercase letters, digits and _,"
            " starting with a letter, at most 48 characters"
        )
    return output_table_prefix(namespace) + output_table


def _plain(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    item = getattr(value, "item", None)
    return item() if callable(item) else str(value)


_ROLE_RE = re.compile(r"[a-z][a-z0-9_]{0,31}")


def describe_cross_source(code: str) -> dict:
    """A cross-source script's declarations as plain data. Meant to run in a worker:
    executor.call("tradingkit.aggregation:describe_cross_source")."""
    agg = ScriptAggregation(code)
    ns = agg._ns
    schema = agg.output_schema()
    inputs = agg.inputs()
    for role in inputs:
        if not _ROLE_RE.fullmatch(role):
            raise ValueError(f"INPUTS role {role!r}: use lowercase letters, digits and _, starting with a letter")
    return {
        "inputs": inputs,
        "input_fields": {str(k): str(v) for k, v in (ns.get("INPUT_FIELDS") or {}).items()},
        "output_table": str(ns.get("OUTPUT_TABLE") or ""),
        "combine_sql": None if agg.combine_sql() is None else str(agg.combine_sql()),
        "output_schema": {str(k): str(v) for k, v in schema.items()} if schema else None,
        "source_filters": {str(k): _plain(v) for k, v in (ns.get("SOURCE_FILTERS") or {}).items()},
        "sources": {
            str(alias): {
                "table": str(ref.table), "field": str(ref.field), "ch_type": str(ref.ch_type),
                "filters": {str(k): _plain(v) for k, v in ref.filters.items()},
            }
            for alias, ref in agg.required_sources().items()
        },
        "has_combine": callable(ns.get("combine")),
    }


async def read_input(
    db: Any, table: str, filters: dict, start_ts: int | None, end_ts: int | None, warmup: int = 0,
) -> pl.DataFrame:
    """A bound input's rows from start_ts to end_ts, after the `warmup` rows before them."""
    rows = await db.fetch_from_unit_table(table, start_ts=start_ts, end_ts=end_ts, filters=filters)
    if not warmup or not start_ts:
        return rows
    earlier = await db.fetch_from_unit_table(
        table, end_ts=start_ts - 1, filters=filters, limit=warmup, newest_first=True,
    )
    if earlier.is_empty():
        return rows
    return earlier if rows.is_empty() else pl.concat([earlier, rows], how="diagonal_relaxed")


class _HostAggregationContext:
    """`ctx` for combine() in a worker: query() asks the host, which decides what a script
    may read."""

    def __init__(self, host: Any, source_filters: dict) -> None:
        self._host = host
        self._source_filters = dict(source_filters or {})

    async def query(
        self, table: str, symbol: str | None = None,
        start_ts: int | None = None, end_ts: int | None = None, **filters: Any,
    ) -> pl.DataFrame:
        if self._host is None:
            raise RuntimeError("ctx.query() needs the host")
        raw = self._host.request(
            "rows", table=table, symbol=symbol, start_ts=start_ts, end_ts=end_ts,
            filters={**self._source_filters, **{k: _plain(v) for k, v in filters.items()}},
        )
        return pl.read_ipc(io.BytesIO(raw)) if raw else pl.DataFrame()

    async def input(
        self, role: str, start_ts: int | None = None, end_ts: int | None = None, warmup: int = 0,
    ) -> pl.DataFrame:
        if self._host is None:
            raise RuntimeError("ctx.input() needs the host")
        raw = self._host.request("input", role=str(role), start_ts=start_ts, end_ts=end_ts, warmup=int(warmup))
        return pl.read_ipc(io.BytesIO(raw)) if raw else pl.DataFrame()


async def run_cross_source_combine(
    code: str, start_ts: int, end_ts: int, source_filters: dict, host: Any = None,
    params: dict | None = None,
) -> list[dict]:
    """Run a script's combine() and return its rows as plain data. Meant to run in a
    worker: executor.call("tradingkit.aggregation:run_cross_source_combine")."""
    script = ScriptAggregation(code, params=params)
    rows = await script.combine(_HostAggregationContext(host, source_filters), start_ts, end_ts)
    if isinstance(rows, pl.DataFrame):
        rows = rows.to_dicts()
    return [{str(k): _plain(v) for k, v in row.items()} for row in rows or []]


class DescribedAggregation(Aggregation):
    """A cross-source script built from describe_cross_source(): the host sets it up and
    reads it without running the script."""

    def __init__(
        self, described: dict, output_table: str, inputs: dict | None = None, params: dict | None = None,
    ) -> None:
        self._described = described
        self._output_table = output_table
        self.params = dict(params or {})
        #: role -> (table, filters): what a run of a script with INPUTS is bound to.
        self.bound_inputs = dict(inputs or {})
        self.SOURCE_FILTERS = dict(described.get("source_filters") or {})

    @property
    def OUTPUT_TABLE(self) -> str:
        return self._output_table

    def required_sources(self) -> dict[str, SourceRef]:  # type: ignore[override]
        if self.bound_inputs:
            fields = self._described.get("input_fields") or {}
            return {role: SourceRef(table, field=fields.get(role, "close"), filters=filters)
                    for role, (table, filters) in self.bound_inputs.items()}
        return {alias: SourceRef(**ref) for alias, ref in self._described["sources"].items()}

    def combine_sql(self) -> str | None:
        return self._described.get("combine_sql")

    def output_schema(self) -> dict[str, str] | None:
        return self._described.get("output_schema")

    def mv_names(self) -> set[str]:
        """Materialized Views setup_aggregation() creates for this aggregation."""
        if self.combine_sql() is None:
            return set()
        return {f"mv_{ref.table}_to_{self._output_table}_{alias}"
                for alias, ref in self.required_sources().items()}


# ---------------------------------------------------------------------------
# AggregationWorker
# ---------------------------------------------------------------------------

class AggregationWorker:
    """
    Keeps every cross-source aggregation script in plugin_library current.

    Scripts run in the executor's workers, never in this process: the host only receives
    their declarations and their rows as plain data, and answers ctx.query() itself,
    reading only tables `table_allowed` accepts. Each project's output goes to its own
    table (output_table_name()).

    combine_sql() scripts get their Materialized Views installed once per version of the
    code; MVs whose script is gone or changed are dropped. combine() scripts run every
    `poll_interval` seconds over the rows that arrived since their last output.

        worker = AggregationWorker(db, executor=executor, table_allowed=allowed)
        await worker.start()
        ...
        await worker.stop()
    """

    def __init__(
        self,
        db: ClickHouseManager,
        executor: Any = None,
        table_allowed: Callable[[str], bool] | None = None,
        poll_interval: float = _POLL_INTERVAL,
        max_warmup: int = 100_000,
    ) -> None:
        if executor is None:
            from tradingkit.executor.local import LocalExecutor
            executor = LocalExecutor()
        self._db = db
        self._executor = executor
        self._table_allowed = table_allowed or (lambda table: True)
        self._poll_interval = poll_interval
        self._max_warmup = max_warmup
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._described: dict[str, dict] = {}      # code hash -> description
        self._installed: dict[str, str] = {}       # output table -> code hash set up
        self._mvs: dict[tuple[str, str], set[str]] = {}  # (namespace, name) -> its MVs
        #: Last outcome per (namespace, name): None when it ran, else the error.
        self.status: dict[tuple[str, str], str | None] = {}

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run_loop(), name="aggregation-worker")
        logger.info("AggregationWorker started")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("AggregationWorker stopped")

    async def _run_loop(self) -> None:
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"AggregationWorker error: {e}")
            try:
                await asyncio.sleep(self._poll_interval)
            except asyncio.CancelledError:
                break

    async def run_once(self) -> None:
        """One pass over every cross-source script, then drop orphaned MVs."""
        async with self._lock:
            await self._run_once()

    async def drop_project(self, namespace: str) -> None:
        """Remove a project's outputs: the MVs feeding them first, since ClickHouse refuses
        inserts into a source whose MV has lost its table, then the tables. Call it once
        the project's scripts are gone from plugin_library."""
        prefix = output_table_prefix(namespace)
        async with self._lock:
            for name in await self._agg_mvs():
                if f"_to_{prefix}" in name:
                    await self._db._execute(f"DROP VIEW IF EXISTS {name}")
            rows = await self._db._execute(
                "SELECT name FROM system.tables WHERE database = currentDatabase()"
                " AND startsWith(name, %(prefix)s)", {"prefix": prefix},
            )
            for (table,) in rows:
                await self._db._execute(f"DROP TABLE IF EXISTS {table}")
            self._installed = {t: d for t, d in self._installed.items() if not t.startswith(prefix)}
            for key in [k for k in self._mvs if k[0] == namespace]:
                del self._mvs[key]
            for key in [k for k in self.status if k[0] == namespace]:
                del self.status[key]

    async def _run_once(self) -> None:
        rows = await self._load_rows()
        scripts = {(ns, name): code for ns, type_, name, code in rows if type_ == "aggregation"}
        seen = set()
        for (namespace, name), code in scripts.items():
            key = (namespace, name)
            try:
                described = await self._describe_code(code)
                if described["inputs"]:
                    continue  # a template: it runs once per run that binds its inputs
                seen.add(key)
                await self._run_job(key, code, await self.describe(namespace, code))
            except Exception as e:
                seen.add(key)
                self.status[key] = str(e)
                logger.error(f"Aggregation {namespace}/{name} failed: {e}")
        for namespace, type_, name, spec_json in rows:
            if type_ != INSTANCE_TYPE:
                continue
            key = (namespace, name)
            seen.add(key)
            try:
                spec = json.loads(spec_json)
                code = scripts.get((namespace, spec.get("script", "")))
                if code is None:
                    raise ValueError(f"Its script {spec.get('script')!r} is gone")
                agg = await self.describe_run(namespace, name, code, spec.get("inputs") or {}, spec.get("params") or {})
                await self._run_job(key, code, agg)
            except Exception as e:
                self.status[key] = str(e)
                logger.error(f"Aggregation run {namespace}/{name} failed: {e}")
        for key in list(self._mvs):
            if key not in seen:
                del self._mvs[key]
        for key in list(self.status):
            if key not in seen:
                del self.status[key]
        await self._drop_orphaned_mvs(set().union(*self._mvs.values()) if self._mvs else set())

    async def _run_job(self, key: tuple[str, str], code: str, agg: DescribedAggregation) -> None:
        namespace = key[0]
        if agg.combine_sql() is not None:
            await self._install(agg, code)
            self._mvs[key] = agg.mv_names()
        elif self._described[_code_hash(code)]["has_combine"]:
            self._mvs.pop(key, None)
            await self._combine(namespace, code, agg)
        else:
            raise ValueError("The script defines neither COMBINE_SQL nor combine()")
        self.status[key] = None

    async def _describe_code(self, code: str) -> dict:
        digest = _code_hash(code)
        if digest not in self._described:
            self._described[digest] = await self._executor.call(
                "tradingkit.aggregation:describe_cross_source", {"code": code},
            )
        return self._described[digest]

    async def describe_run(
        self, namespace: str, name: str, code: str, inputs: dict[str, str], params: dict | None = None,
    ) -> DescribedAggregation:
        """A run of a script with INPUTS: each role bound to a connection of the project,
        its own values for the script's __params__, writing to a table named after the run."""
        from tradingkit.core.script_ast import resolve_params, script_params
        declared = script_params(code)
        unknown = sorted(set(params or {}) - set(declared))
        if unknown:
            raise ValueError(f"The script declares no parameter {', '.join(unknown)}")
        values = resolve_params(code, params)
        described = await self._describe_code(code)
        roles = described["inputs"]
        if not roles:
            raise ValueError("The script declares no INPUTS, so it runs by itself")
        missing = [role for role in roles if not inputs.get(role)]
        if missing:
            raise ValueError(f"Choose a connection for {', '.join(missing)}")
        bound = {role: await self.connection_input(namespace, inputs[role]) for role in roles}
        for role, (table, _) in bound.items():
            if not self._table_allowed(table):
                raise ValueError(f"Input {role!r} reads table {table!r}, which aggregations may not read")
        return DescribedAggregation(described, output_table_name(namespace, name), bound, values)

    async def connection_input(self, namespace: str, connection: str) -> tuple[str, dict]:
        """(table, filters) holding exactly the rows a connection of the project collects."""
        from tradingkit.collector import connection_storage
        from tradingkit.source import ScriptSource
        conn = next((c for c in await self._db.list_connections()
                     if c["name"] == connection and c["project"] == namespace), None)
        if conn is None:
            raise ValueError(f"No connection {connection!r} in this project")
        code = await self._db.get_plugin_code(namespace, "source", conn["source"])
        if not code:
            raise ValueError(f"Connection {connection!r}: its source {conn['source']!r} is gone")
        storage = connection_storage(conn, ScriptSource(code=code))
        return storage.table, storage.filters

    async def drop_output(self, namespace: str, name: str) -> None:
        """Forget what a script or run computed, so it starts over: its views, its table
        and its status."""
        table = output_table_name(namespace, name)
        async with self._lock:
            await self._drop_mvs_into(table)
            await self._db._execute(f"DROP TABLE IF EXISTS {table}")
            self._installed.pop(table, None)
            self._mvs.pop((namespace, name), None)
            self.status.pop((namespace, name), None)

    async def describe(self, namespace: str, code: str) -> DescribedAggregation:
        """The script's declarations, read in a worker and cached per version of the code."""
        described = await self._describe_code(code)
        agg = DescribedAggregation(described, output_table_name(namespace, described["output_table"]))
        for alias, ref in agg.required_sources().items():
            if not self._table_allowed(ref.table):
                raise ValueError(f"Source {alias!r} reads table {ref.table!r}, which aggregations may not read")
        return agg

    async def _install(self, agg: DescribedAggregation, code: str) -> None:
        table = agg.OUTPUT_TABLE
        digest = _code_hash(code + json.dumps(agg.bound_inputs, sort_keys=True))
        if self._installed.get(table) == digest:
            return
        aliases = set(agg.required_sources())
        try:
            columns = set(await self._db.get_table_schema(table)) - {"timestamp"}
        except Exception:
            columns = set()
        if columns and columns != aliases:
            # The sources changed: the alignment table has a column per source, so it is
            # rebuilt; the backfill in setup_aggregation() refills it.
            await self._drop_mvs_into(table)
            await self._db._execute(f"DROP TABLE IF EXISTS {table}")
        await setup_aggregation(self._db, agg)
        self._installed[table] = digest

    async def _combine(self, namespace: str, code: str, agg: DescribedAggregation) -> None:
        table = agg.OUTPUT_TABLE
        now_ts = int(time.time())
        last_ts = await self._get_last_ts(table, namespace) or 0
        if now_ts <= last_ts:
            return
        rows = await self._executor.call(
            "tradingkit.aggregation:run_cross_source_combine",
            {"code": code, "start_ts": last_ts, "end_ts": now_ts, "source_filters": agg.SOURCE_FILTERS,
             "params": agg.params},
            on_request=functools.partial(self.serve, inputs=agg.bound_inputs),
        )
        if not rows:
            return
        df = pl.DataFrame(rows)
        if "timestamp" not in df.columns:
            raise ValueError("combine() rows need a 'timestamp'")
        schema = agg.output_schema()
        if schema:
            df = df.cast({k: _PL_TYPES.get(v, pl.String) for k, v in schema.items() if k in df.columns})
        await self._db.ensure_raw_table(table, dict(zip(df.columns, df.dtypes)))
        await self._db.insert_unit_batch(table, df, namespace, "", "")
        await self._db.flush()
        logger.info(f"AggregationWorker: {namespace} → {table} stored {len(rows)} rows")

    async def serve(self, ask: dict, inputs: dict[str, tuple[str, dict]] | None = None) -> bytes:
        """Answer a script's ctx.query() and ctx.input(): rows of a table `table_allowed`
        accepts, as Arrow IPC. Pass it as `on_request` to run a script outside the worker,
        with `inputs` the run's bound roles (DescribedAggregation.bound_inputs)."""
        kind = ask.get("kind")
        if kind == "input":
            role = str(ask.get("role") or "")
            if role not in (inputs or {}):
                raise ValueError(f"No input {role!r}; this run binds {sorted(inputs or {})}")
            table, filters = inputs[role]
            warmup = max(0, min(int(ask.get("warmup") or 0), self._max_warmup))
            df = await read_input(self._db, table, filters, ask.get("start_ts"), ask.get("end_ts"), warmup)
            buffer = io.BytesIO()
            df.write_ipc(buffer)
            return buffer.getvalue()
        if kind != "rows":
            raise ValueError(f"Unknown data request: {kind!r}")
        table = str(ask.get("table") or "")
        if not self._table_allowed(table):
            raise ValueError(f"Aggregations may not read table {table!r}")
        df = await self._db.fetch_from_unit_table(
            table, symbol=ask.get("symbol"), start_ts=ask.get("start_ts"), end_ts=ask.get("end_ts"),
            filters=ask.get("filters") or {},
        )
        buffer = io.BytesIO()
        df.write_ipc(buffer)
        return buffer.getvalue()

    async def _load_rows(self) -> list[tuple[str, str, str, str]]:
        """(namespace, type, name, code) of every cross-source script and every run.

        Within-table aggregation(unit) scripts share type='aggregation' but are folded at
        query time instead, and the built-in templates are starting points, not jobs.
        """
        if not self._db._conn:
            return []
        rows = await self._db._execute(
            "SELECT namespace, type, name, code FROM plugin_library FINAL"
            " WHERE type IN ('aggregation', %(run)s)", {"run": INSTANCE_TYPE},
        )
        return [(ns, type_, name, code) for ns, type_, name, code in rows
                if type_ == INSTANCE_TYPE or not AggregationScript(code).is_ch_mv()]

    async def _drop_mvs_into(self, table: str) -> None:
        for name in await self._agg_mvs():
            if f"_to_{table}_" in name:
                await self._db._execute(f"DROP VIEW IF EXISTS {name}")

    async def _drop_orphaned_mvs(self, keep: set[str]) -> None:
        for name in await self._agg_mvs():
            if name not in keep:
                await self._db._execute(f"DROP VIEW IF EXISTS {name}")
                logger.info(f"AggregationWorker: dropped {name}, its script is gone or changed")
        # A table whose MVs were dropped is set up again if its script comes back.
        self._installed = {t: d for t, d in self._installed.items()
                           if any(f"_to_{t}_" in mv for mv in keep)}

    async def _agg_mvs(self) -> list[str]:
        """Materialized Views writing into aggregation output tables (agg_*)."""
        rows = await self._db._execute(
            "SELECT name FROM system.tables WHERE database = currentDatabase()"
            " AND engine = 'MaterializedView' AND startsWith(name, 'mv_') AND position(name, '_to_agg_') > 0"
        )
        return [r[0] for r in rows]

    async def _get_last_ts(self, table: str, exchange: str) -> int | None:
        """max(timestamp) of the rows this worker stored for `exchange`, None when none."""
        try:
            rows = await self._db._execute(
                f"SELECT max(timestamp) FROM {table} WHERE exchange = %(ex)s", {"ex": exchange},
            )
        except Exception:
            return None
        val = rows[0][0] if rows else None
        return int(val) if val else None


def _code_hash(code: str) -> str:
    return hashlib.sha256(code.encode()).hexdigest()


# ---------------------------------------------------------------------------
# AggregationScript — within-table bucket-aggregation script wrapper (unrelated to
# Aggregation/ScriptAggregation above — see module docstring)
# ---------------------------------------------------------------------------

class AggregationScript:
    """
    Wrapper around a within-table bucket-aggregation script (`aggregation(unit) ->
    AggSpec`, tradingkit/schema.py's Fold machinery). fold() builds bars of any timeframe
    from the stored rows at query time. DataCollector can also materialize `buckets` into
    per-bucket tables, but a Materialized View sees every insert: a bar re-inserted while
    it forms is folded again, so sums over a "replace" stream come out inflated.

    NOT the cross-table mechanism — that's Aggregation/ScriptAggregation above. This class
    used to also wrap the cross-table aggregate(ctx, start_ts, end_ts) convention; that
    half has been removed in favor of Aggregation/ScriptAggregation, which cover the same
    ground plus combine_sql()'s MV-native fast path.

    Type is detected via AST — no exec needed for introspection:
      is_ch_mv()   → True when aggregation(unit) is defined
    """

    def __init__(self, code: str) -> None:
        from tradingkit.core.script_ast import ScriptAST
        self._code = code
        self._ast = ScriptAST(code)

    def is_ch_mv(self) -> bool:
        return self._ast.has_function("aggregation")

    def get_source_table(self) -> str | None:
        return self._ast.get_literal("SOURCE_TABLE")

    def get_agg_spec_for_unit(self, unit: Any) -> Any | None:
        try:
            return self.spec_for_unit(unit)
        except Exception:
            return None

    def spec_for_unit(self, unit: Any) -> Any:
        """The AggSpec the script returns for `unit`; raises when the script is broken."""
        from tradingkit.schema import AggSpec
        ns: dict = {}
        exec(compile(self._code, "<aggregation_script>", "exec"), ns)  # noqa: S102
        if "aggregation" not in ns:
            raise ValueError("The script defines no aggregation(unit) function")
        spec = ns["aggregation"](unit)
        if not isinstance(spec, AggSpec):
            raise TypeError(f"aggregation(unit) must return AggSpec, got {type(spec).__name__}")
        return spec

    async def spec_for_table(self, db: Any, executor: Any = None) -> tuple[str, Any]:
        """The source table and the AggSpec built from that table's columns. With an
        executor, the script runs in one of its workers rather than in this process."""
        from tradingkit.schema import AggSpec, DataUnit
        table = self.get_source_table() or "candles"
        schema = await db.get_table_schema(table)
        if not schema:
            raise ValueError(f"Source table {table!r} does not exist yet")
        if executor is None:
            return table, self.spec_for_unit(DataUnit(schema))
        described = await executor.call(
            "tradingkit.aggregation:describe_aggregation_spec",
            {"code": self._code, "schema": {col: str(dtype) for col, dtype in schema.items()}},
        )
        return table, AggSpec.from_plain(described)

    async def fold(
        self,
        db: Any,
        exchange: str,
        symbol: str,
        timeframe: str,
        *,
        start_ts: int | None = None,
        end_ts: int | None = None,
        limit: int | None = None,
        newest_first: bool = False,
        executor: Any = None,
    ) -> list[dict]:
        """Bars of any `timeframe`, folded at query time from the market's stored rows.

        The timeframe must be a whole multiple of the finest stored timeframe.
        """
        from tradingkit.core.timeframe import parse_timeframe
        table, spec = await self.spec_for_table(db, executor)
        base = await db.base_timeframe(table, exchange, symbol)
        if base is None:
            return []
        bucket_s = parse_timeframe(timeframe)
        if bucket_s % parse_timeframe(base):
            raise ValueError(f"{timeframe} is not a whole multiple of the stored {base} rows")
        return await db.fold_buckets(
            table, spec.folds, bucket_s, exchange, symbol, base,
            start_ts=start_ts, end_ts=end_ts, limit=limit, newest_first=newest_first,
        )


def describe_aggregation_spec(code: str, schema: dict[str, str]) -> dict:
    """The AggSpec a script returns for a table schema, as plain data. Meant to run in a
    worker: SubprocessExecutor.call("tradingkit.aggregation:describe_aggregation_spec")."""
    from tradingkit.schema import DataUnit
    dtypes = {col: getattr(pl, name, pl.String) for col, name in schema.items()}
    return AggregationScript(code).spec_for_unit(DataUnit(dtypes)).to_plain()


# Empty by default -- a host app can register its own via get_builtin_registry()
# (tradingkit.core.plugin_registry), see AggregationWorker._load_python_scripts() and
# load_aggregation_plugin() above.
BUILTIN_AGGREGATIONS: dict[str, str] = {}


__all__ = [
    "BUILTIN_AGGREGATIONS",
    "Aggregation",
    "AggregationContext",
    "AggregationScript",
    "AggregationWorker",
    "ScriptAggregation",
    "DescribedAggregation",
    "SourceRef",
    "describe_aggregation_spec",
    "describe_cross_source",
    "output_table_name",
    "output_table_prefix",
    "run_cross_source_combine",
    "load_aggregation_plugin",
    "query_aggregation",
    "setup_aggregation",
]
