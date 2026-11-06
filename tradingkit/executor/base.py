"""
tradingkit.executor.base — PluginExecutor ABC.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, Self

import polars as pl

if TYPE_CHECKING:
    from tradingkit.executor.cpp_pool import CppRunnerPool
    from tradingkit.indicator import Indicator, IndicatorContext
    from tradingkit.sink import Sink
    from tradingkit.source import DataSource
    from tradingkit.strategy import BarContext, Signal, Strategy


class SourceSession:
    """One source instance kept alive for a connection: it is set up once and its HTTP
    session and state survive between calls."""

    def __init__(self, source: DataSource) -> None:
        self._source = source

    async def fetch(
        self,
        symbol: str,
        timeframe: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
        stream: str | None = None,
    ) -> pl.DataFrame:
        return await self._source.get_historical_data(
            symbol, timeframe, start_ts, end_ts, limit, stream,
        )

    async def stream(
        self,
        symbol: str,
        timeframe: int,
        on_event: Callable[[dict], None] | None = None,
        stream: str | None = None,
    ) -> AsyncIterator[dict]:
        async for row in self._source.stream(symbol, timeframe, on_event, stream):
            yield row

    async def batch_gaps(self, gaps: list[dict]) -> list[dict]:
        return self._source.batch_gaps(gaps)

    async def close(self) -> None:
        await self._source.shutdown()


class SinkSession:
    """One sink instance kept alive: its connection survives between deliveries."""

    def __init__(self, sink: Sink) -> None:
        self._sink = sink

    async def deliver(self, event: dict) -> None:
        await self._sink.deliver(event)

    async def close(self) -> None:
        await self._sink.shutdown()


class PluginExecutor(ABC):
    """
    Backend for running indicators, strategies, and data sources.

    Implementations:
        LocalExecutor      — in-process, no isolation
        SubprocessExecutor — Arrow IPC sandbox
        RemoteExecutor     — generic HTTP (tradingkit-runner server)

    Optionally attach a CppRunnerPool to enable CppIndicator execution:
        executor = SubprocessExecutor(cpp_pool=CppRunnerPool())
    """

    #: Optional C++ runner pool — set to enable CppIndicator execution.
    cpp_pool: CppRunnerPool | None = None

    @abstractmethod
    async def compute_indicator(
        self,
        indicator: Indicator,
        ctx: IndicatorContext,
    ) -> pl.Series:
        """
        Run indicator.compute(ctx) and return pl.Series of Float64.
        Length == len(ctx). First required_periods() values will be NaN.
        """
        ...

    @abstractmethod
    async def process_strategy_bar(
        self,
        strategy: Strategy,
        bar: BarContext,
    ) -> Signal | None:
        """
        Call strategy.on_bar(bar) and return Signal or None.
        """
        ...

    @abstractmethod
    async def fetch_source_data(
        self,
        source: DataSource,
        symbol: str,
        timeframe: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
    ) -> pl.DataFrame:
        """
        Call source.get_historical_data() and return pl.DataFrame.
        """
        ...

    async def run_strategy_script(
        self, code: str, candles: list[dict], params: dict | None = None, name: str = "strategy",
    ) -> list[dict]:
        """Run a vectorized strategy script over bars. Default: in-process."""
        from tradingkit.strategy import run_strategy_script
        return run_strategy_script(code, candles, params, name)

    async def open_sink(self, sink: Sink) -> SinkSession:
        """A long-lived handle on one sink instance. Default: in-process."""
        return SinkSession(sink)

    async def open_source(self, source: DataSource) -> SourceSession:
        """A long-lived handle on one source instance. Default: in-process."""
        return SourceSession(source)

    async def start(self) -> None:
        """Optional: initialize executor resources (subprocesses, connections...)."""

    async def stop(self) -> None:
        """Optional: release executor resources."""

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_) -> None:
        await self.stop()
