"""
tradingkit.source — DataSource and ScriptSource.

A source plugin subclasses DataSource, describes its data as named streams, and
implements one request (history) and one connection (live). The framework owns
everything around them: one-time setup, the HTTP session, retries, rate limits and the
reconnect loop.

Usage:
    from tradingkit import DataSource
    from tradingkit.source import ScriptSource
"""
from __future__ import annotations

import asyncio
import inspect
import itertools
import logging
import time
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

import aiohttp
import numpy as np
import polars as pl

from tradingkit.core.plugin_runtime import PluginRuntime
from tradingkit.core.script_ast import resolve_params
from tradingkit.core.timeframe import parse_timeframe, seconds_to_tf_string

logger = logging.getLogger(__name__)


class TransientSourceError(Exception):
    """Raise from history() for a failure worth retrying, e.g. an exchange-side error."""


_RETRYABLE = (aiohttp.ClientError, asyncio.TimeoutError, OSError, TransientSourceError)


# ------------------------------------------------------------------ #
# Stream                                                               #
# ------------------------------------------------------------------ #

@dataclass(frozen=True)
class Stream:
    """
    What a stream of data is like. The framework derives its behaviour from these
    properties rather than from a fixed list of data types.

    table:    where rows are stored.
    columns:  expected columns, as {name: type}. Rows missing one are rejected.
    step:     distance between rows, in the source's timestamp unit. A stream with a step
              gets gap recovery; None means rows arrive irregularly.
    updates:  "replace" when a new row for a timestamp overwrites the stored one (a
              forming candle), "append" when every row is its own record (a trade).
    history:  whether history() can serve this stream.
    """

    table: str | None = None
    columns: dict[str, Any] | None = None
    step: int | None = None
    updates: Literal["replace", "append"] = "replace"
    history: bool = True

    def __post_init__(self) -> None:
        # Streams are read from a script without running it, so a step written as a name
        # (step=QUARTER) arrives as the string "QUARTER".
        if self.step is not None and (isinstance(self.step, bool) or not isinstance(self.step, int) or self.step <= 0):
            raise ValueError(f"Stream step must be a positive whole number written in place, not {self.step!r}")
        if self.updates not in ("replace", "append"):
            raise ValueError(f"Stream updates must be 'replace' or 'append', not {self.updates!r}")
        if self.table is not None and not isinstance(self.table, str):
            raise ValueError(f"Stream table must be a name in quotes, not {self.table!r}")

    def check(self, columns: Any) -> None:
        if not self.columns:
            return
        missing = set(self.columns) - set(columns)
        if missing:
            raise ValueError(f"Rows for {self.table} are missing columns: {sorted(missing)}")


DEFAULT_STREAM = "default"


# ------------------------------------------------------------------ #
# DataSource                                                           #
# ------------------------------------------------------------------ #

class DataSource(PluginRuntime, ABC):
    """
    Base class for all data sources (exchanges, CSV files, APIs, custom feeds...).

    Declare `streams`, implement history(), and live() for live data. Optionally
    implement setup() and close() for work that should happen once rather than on every
    call. A source that only serves one stream may leave `streams` empty.

    `timeframe` throughout is an integer *step* in this source's own timestamp unit —
    not necessarily seconds (see timestamp_unit). It also picks the stream when none is
    named: the one with that step, otherwise the first declared.

    Behaviour is tuned through config: max_retries, retry_delay_s, request_timeout_s,
    reconnect_delay_s, max_reconnect_delay_s, stale_timeout_s, and the rate limit keys
    read by rate_limit().
    """

    #: Unit of this source's `timestamp` column. The framework never uses it in
    #: arithmetic — gap/batch math is unit-relative, correct in any unit as long as
    #: timestamps and `timeframe` share it. It is consulted only where wall-clock time
    #: must be converted into data timestamps, and for display.
    timestamp_unit: str = "s"

    #: Named streams this source serves.
    streams: ClassVar[dict[str, Stream]] = {}

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._request_times: list[float] = []
        self._last_request: float = 0.0

    # ------------------------------------------------------------------ #
    # Plugin hooks                                                         #
    # ------------------------------------------------------------------ #

    def stream_specs(self) -> dict[str, Stream]:
        return self.streams

    def resolve_stream(self, stream: str | None = None, timeframe: int | None = None) -> str:
        specs = self.stream_specs()
        if stream is not None:
            if stream not in specs and specs:
                raise ValueError(f"Unknown stream {stream!r}. Declared: {sorted(specs)}")
            return stream
        for name, spec in specs.items():
            if timeframe is not None and spec.step == timeframe:
                return name
        return next(iter(specs), DEFAULT_STREAM)

    def stream_spec(self, stream: str | None = None, timeframe: int | None = None) -> Stream:
        return self.stream_specs().get(self.resolve_stream(stream, timeframe), Stream())

    def has_live(self) -> bool:
        return type(self).live is not DataSource.live

    def has_history(self) -> bool:
        return True

    @abstractmethod
    async def history(
        self, stream: str, symbol: str, start_ts: int, end_ts: int, limit: int,
    ) -> pl.DataFrame | list[dict]:
        """
        One request for rows of `stream` in [start_ts, end_ts]. No retries or throttling
        here.

        Pipeline consumers (IndicatorContext, BarContext) expect OHLCV columns
        (timestamp, open, high, low, close, volume).
        """
        ...

    async def live(self, stream: str, symbol: str) -> AsyncIterator[dict]:
        """
        One live connection: yields row dicts until it ends. Raising or returning hands
        control back to the framework, which reconnects. Override for live sources.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support live data")
        yield {}

    # ------------------------------------------------------------------ #
    # Framework services                                                   #
    # ------------------------------------------------------------------ #

    async def get_historical_data(
        self,
        symbol: str,
        timeframe: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
        stream: str | None = None,
    ) -> pl.DataFrame:
        """history() with setup, rate limiting and retries applied."""
        await self.open()
        name = self.resolve_stream(stream, timeframe)
        spec = self.stream_spec(name)
        attempts = int(self.config.get("max_retries", 5))
        delay = float(self.config.get("retry_delay_s", 1))
        for attempt in range(attempts):
            await self.rate_limit()
            try:
                result = await self.history(name, symbol, start_ts, end_ts, limit)
                df = result if isinstance(result, pl.DataFrame) else pl.DataFrame(result or [])
                if df.height:
                    spec.check(df.columns)
                return df
            except _RETRYABLE as exc:
                if attempt == attempts - 1:
                    raise
                logger.warning(f"{symbol}: history failed ({exc!r}), retrying")
                await asyncio.sleep(delay * 2 ** attempt)
        return pl.DataFrame()

    async def stream(
        self,
        symbol: str,
        timeframe: int,
        on_event: Callable[[dict], None] | None = None,
        stream: str | None = None,
    ) -> AsyncIterator[dict]:
        """live() kept alive: reconnects with backoff, and treats a silent connection as
        dead. `on_event` receives {"event": "reconnecting", "error": ...} and
        {"event": "connected"}."""
        await self.open()
        name = self.resolve_stream(stream, timeframe)
        spec = self.stream_spec(name)
        emit = on_event or (lambda event: None)
        base_delay = float(self.config.get("reconnect_delay_s", 1))
        max_delay  = float(self.config.get("max_reconnect_delay_s", 60))
        stale      = float(self.config.get("stale_timeout_s", 300))
        delay = base_delay
        failed = False

        while True:
            rows = self.live(name, symbol).__aiter__()
            try:
                while True:
                    row = await asyncio.wait_for(rows.__anext__(), stale)
                    spec.check(row)
                    delay = base_delay
                    if failed:
                        failed = False
                        emit({"event": "connected"})
                    yield row
            except StopAsyncIteration:
                reason = "connection closed"
            except TimeoutError:
                reason = f"no data for {stale:.0f}s"
            except NotImplementedError:
                raise
            except Exception as exc:
                reason = repr(exc)
            finally:
                await rows.aclose()

            failed = True
            emit({"event": "reconnecting", "error": reason})
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_delay)

    # ------------------------------------------------------------------ #
    # Gap recovery                                                         #
    # ------------------------------------------------------------------ #

    async def detect_gaps(self, rows: list[dict], timeframe: int) -> list[dict]:
        """
        Find missing bars in a timestamp-ordered row sequence at a fixed timeframe.

        Default: flag any gap between consecutive rows. Override for source-specific
        gap semantics (e.g. skip known exchange maintenance windows), or return []
        to disable gap detection for this source entirely.

        Returns a list of {"start_timestamp", "end_timestamp", "missing_rows"} dicts.
        """
        gaps: list[dict] = []
        for prev_row, row in itertools.pairwise(rows):
            # Integer division tolerates small jitter and delta<=0 (e.g. a source
            # re-pushing the same still-forming bar) without a separate tolerance param.
            missing = int(row["timestamp"] - prev_row["timestamp"]) // timeframe - 1
            if missing > 0:
                gaps.append({
                    "start_timestamp": prev_row["timestamp"] + timeframe,
                    "end_timestamp":   row["timestamp"] - timeframe,
                    "missing_rows":    missing,
                })
        return gaps

    def batch_gaps(self, gaps: list[dict]) -> list[dict]:
        """
        Merge nearby gaps into one fetch, split apart gaps too big or too far apart to
        fetch in one request.

        Default reads gap_max_batch/gap_max_time_gap from self.config. Override for
        source-specific batching, e.g. an exchange with a tight rate limit wanting
        smaller batches.
        """
        if not gaps:
            return []
        max_batch    = self.config.get("gap_max_batch", 1440)
        max_time_gap = self.config.get("gap_max_time_gap", 3600)
        sorted_gaps = sorted(gaps, key=lambda g: g["start_timestamp"])
        batches: list[dict] = []
        cur = {
            "start_timestamp": sorted_gaps[0]["start_timestamp"],
            "end_timestamp":   sorted_gaps[0]["end_timestamp"],
            "total_rows":      sorted_gaps[0]["missing_rows"],
        }
        for gap in sorted_gaps[1:]:
            time_gap = gap["start_timestamp"] - cur["end_timestamp"]
            if (cur["total_rows"] + gap["missing_rows"] <= max_batch
                    and time_gap <= max_time_gap):
                cur["end_timestamp"] = gap["end_timestamp"]
                cur["total_rows"]   += gap["missing_rows"]
            else:
                batches.append(cur)
                cur = {
                    "start_timestamp": gap["start_timestamp"],
                    "end_timestamp":   gap["end_timestamp"],
                    "total_rows":      gap["missing_rows"],
                }
        batches.append(cur)
        return batches

    async def fetch_gap(
        self, symbol: str, timeframe: int, start_ts: int, end_ts: int,
    ) -> list[dict]:
        """
        Backfill one already-batched gap range with real data.

        Default: re-fetch via get_historical_data() — not synthetic
        data. NaN-fill and forward-fill were both ruled out: NaN permanently poisons
        every subsequent indicator output, and forward-fill invents prices that never
        happened. Override to backfill from a different source, or return [] to disable
        backfill for this source (the gap is then left unfilled — no worse than before
        gap detection existed, just not fixed).
        """
        df = await self.get_historical_data(symbol, timeframe, start_ts, end_ts)
        return df.to_dicts()

    async def rate_limit(self) -> None:
        """
        Throttle outbound requests. Rate limits are a property of the source (the
        exchange), not of whoever calls it — so DataCollector and Pipeline both get
        them from here instead of each re-implementing throttling.

        Reads max_requests / per_seconds / rate_limit_ms from self.config. Override for
        a different policy (e.g. the weight-based limits some exchanges use).
        """
        max_requests  = int(self.config.get("max_requests", 10000))
        per_seconds   = int(self.config.get("per_seconds", 10))
        rate_limit_ms = float(self.config.get("rate_limit_ms", 50))

        now = time.time()
        self._request_times = [t for t in self._request_times if now - t <= per_seconds]
        if len(self._request_times) >= max_requests:
            wait = per_seconds - (now - min(self._request_times))
            if wait > 0:
                await asyncio.sleep(wait)
        min_delay = rate_limit_ms / 1000.0
        elapsed = now - self._last_request
        if elapsed < min_delay:
            await asyncio.sleep(min_delay - elapsed)
        now = time.time()
        self._request_times.append(now)
        self._last_request = now

    # ------------------------------------------------------------------ #
    # Lifecycle                                                            #
    # ------------------------------------------------------------------ #

    async def initialize(self) -> bool:
        """Optional: initialize connection. Called before first use."""
        return True

    async def cleanup(self) -> None:
        """Optional: release resources."""

    async def health_check(self) -> dict:
        """Optional: connection health check."""
        return {"status": "ok"}

    def validate(self) -> bool:
        return True


# ------------------------------------------------------------------ #
# ScriptSource                                                         #
# ------------------------------------------------------------------ #

class ScriptSource(DataSource):
    """
    A source written as a script: code defining one DataSource subclass.

        __params__ = {"batch_size": {"type": "int", "default": 1440, "label": "Batch size"}}

        class MySource(DataSource):
            streams = {"candles": Stream(table="candles", step=60)}

            async def history(self, stream, symbol, start_ts, end_ts, limit): ...
            async def live(self, stream, symbol): ...

    Each __params__ entry takes type (int, float, bool, str, date), default, label,
    description, required, options, min, max, step and scope. `"scope": "connection"`
    puts a field at the top of the connection form, filled in per connection; without it
    the field is a setting with a default. Mistakes are reported by param_problems().

    The namespace provides DataSource, Stream, TransientSourceError, pl, np,
    parse_timeframe and tf_string (seconds to "1m"). `self.config` is __params__ defaults
    merged with the user-supplied config. `streams`, timestamp_unit and __params__ are
    read from the source text without executing it, so asking about a script never runs
    its code; their values therefore have to be literals.
    """

    def __init__(
        self,
        code: str,
        config: dict | None = None,
        allowed_hosts: list[str] | None = None,
        **params: Any,
    ) -> None:
        super().__init__(config)
        self._code = code
        self._params_cache: dict | None = None
        self._stream_cache: dict[str, Stream] | None = None
        self._ast_cache: Any = None
        self._impl: DataSource | None = None
        self._impl_code: str | None = None
        self.allowed_hosts = allowed_hosts or []
        self.params = params

    @property
    def _ast(self) -> Any:
        """Parsed AST of the script, built on demand. Derived state rather than a
        constructor field so it never travels through pickle — tradingkit-runner's
        unpickling allowlist is a security boundary, and widening it for a cache that
        rebuilds from _code in microseconds would be the wrong trade."""
        from tradingkit.core.script_ast import ScriptAST
        if self._ast_cache is None or self._ast_cache._code != self._code:
            self._ast_cache = ScriptAST(self._code)
        return self._ast_cache

    def __getstate__(self) -> dict:
        # The instance can hold unpicklable objects (HTTP sessions, sockets) and is
        # rebuilt from _code on the other side.
        state = super().__getstate__()
        state["_impl"] = state["_impl_code"] = None
        state["_ast_cache"] = None
        return state

    def _build(self) -> DataSource:
        """Exec the script and instantiate its DataSource subclass, once per code version.
        Isolation comes from the executor: under SubprocessExecutor this runs in the worker."""
        namespace: dict[str, Any] = {
            "DataSource":           DataSource,
            "Stream":               Stream,
            "TransientSourceError": TransientSourceError,
            "pl":                   pl,
            "np":                   np,
            "parse_timeframe":      parse_timeframe,
            "tf_string":            seconds_to_tf_string,
            **self.params,
        }
        exec(compile(self._code, "<source_script>", "exec"), namespace)  # noqa: S102
        classes = [
            v for v in namespace.values()
            if isinstance(v, type) and issubclass(v, DataSource) and v is not DataSource
        ]
        if not classes:
            raise ValueError("Source script must define a DataSource subclass")
        # A script may define a shared base plus a concrete subclass; dict order would
        # otherwise decide which one wins, so take the most-derived explicitly.
        cls = max(classes, key=lambda c: len(c.__mro__))
        takes_config = len(inspect.signature(cls.__init__).parameters) > 1
        return cls(self.merged_config()) if takes_config else cls()

    def _instance(self) -> DataSource:
        if self._impl is None or self._impl_code != self._code:
            self._impl, self._impl_code = self._build(), self._code
        return self._impl

    # ------------------------------------------------------------------ #
    # Declarations — parsed without execution                              #
    # ------------------------------------------------------------------ #

    def extract_params(self) -> dict:
        """Return the __params__ dict declared in the script, or {}."""
        if self._params_cache is not None:
            return self._params_cache
        self._params_cache = self._ast.get_literal("__params__") or {}
        return self._params_cache

    def merged_config(self) -> dict:
        """__params__ defaults merged with user-supplied config values, cast to the
        declared types since a form sends strings."""
        return resolve_params(self._code, self.config)

    def stream_specs(self) -> dict[str, Stream]:
        if self._stream_cache is None:
            self._stream_cache = {
                name: Stream(**fields) for name, fields in self._ast.get_calls_dict("streams").items()
            }
        return self._stream_cache

    def has_live(self) -> bool:
        return self._ast.has_function("live")

    def has_history(self) -> bool:
        return self._ast.has_function("history")

    @property
    def timestamp_unit(self) -> str:  # type: ignore[override]
        return self._ast.get_literal("timestamp_unit") or "s"

    # ------------------------------------------------------------------ #
    # Delegation to the script's instance                                  #
    # ------------------------------------------------------------------ #

    async def history(
        self, stream: str, symbol: str, start_ts: int, end_ts: int, limit: int,
    ) -> pl.DataFrame | list[dict]:
        return await self._instance().history(stream, symbol, start_ts, end_ts, limit)

    async def open(self) -> None:
        await self._instance().open()

    async def shutdown(self) -> None:
        if self._impl is not None:
            await self._impl.shutdown()

    async def get_historical_data(
        self,
        symbol: str,
        timeframe: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
        stream: str | None = None,
    ) -> pl.DataFrame:
        return await self._instance().get_historical_data(
            symbol, timeframe, start_ts, end_ts, limit, stream,
        )

    async def stream(
        self,
        symbol: str,
        timeframe: int,
        on_event: Callable[[dict], None] | None = None,
        stream: str | None = None,
    ) -> AsyncIterator[dict]:
        async for row in self._instance().stream(symbol, timeframe, on_event, stream):
            yield row

    async def detect_gaps(self, rows: list[dict], timeframe: int) -> list[dict]:
        return await self._instance().detect_gaps(rows, timeframe)

    def batch_gaps(self, gaps: list[dict]) -> list[dict]:
        return self._instance().batch_gaps(gaps)

    async def fetch_gap(
        self, symbol: str, timeframe: int, start_ts: int, end_ts: int,
    ) -> list[dict]:
        return await self._instance().fetch_gap(symbol, timeframe, start_ts, end_ts)

    async def rate_limit(self) -> None:
        await self._instance().rate_limit()


__all__ = ["DataSource", "ScriptSource", "Stream", "TransientSourceError"]
