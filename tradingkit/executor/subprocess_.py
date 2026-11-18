"""
tradingkit.executor.subprocess_ — SubprocessExecutor.

Runs plugins in isolated subprocesses:
  - Data transfer via Apache Arrow IPC (zero-copy shared memory where possible)
  - Memory limit enforced via resource module
  - Process-boundary isolation only — no import/builtin restrictions inside the
    subprocess, which shares this host's user, environment and network. Use
    SandboxExecutor to run code you did not write. See the "Security model" section in
    the top-level README.
  - Worker output is decoded with _wire.plain_loads(), never plain pickle
  - Timeout per computation
"""
from __future__ import annotations

import asyncio
import logging
import os
import pickle
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl
import pyarrow as pa

from tradingkit.executor._wire import Channel, spawn_local
from tradingkit.executor.base import PluginExecutor, SinkSession, SourceSession

if TYPE_CHECKING:
    from tradingkit.indicator import Indicator, IndicatorContext
    from tradingkit.sink import Sink
    from tradingkit.source import DataSource
    from tradingkit.strategy import BarContext, Signal, Strategy

logger = logging.getLogger(__name__)

_WORKER_SCRIPT = os.path.join(os.path.dirname(__file__), "_worker.py")
_SESSION_SCRIPT = os.path.join(os.path.dirname(__file__), "_source_worker.py")


def _df_to_arrow_bytes(df: pl.DataFrame) -> bytes:
    """Serialize Polars DataFrame → Arrow IPC bytes."""
    arrow_table = df.to_arrow()
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, arrow_table.schema)
    writer.write_table(arrow_table)
    writer.close()
    return sink.getvalue().to_pybytes()


def _arrow_bytes_to_df(data: bytes) -> pl.DataFrame:
    """Deserialize Arrow IPC bytes → Polars DataFrame."""
    buf = pa.py_buffer(data)
    reader = pa.ipc.open_stream(buf)
    return pl.from_arrow(reader.read_all())


class SubprocessExecutor(PluginExecutor):
    """
    Executes plugins in isolated subprocesses with Arrow IPC data transfer.

    Suitable for user-uploaded plugins and UI editor code (ScriptIndicator,
    ScriptStrategy, ScriptSource) where sandboxing is required.

    Usage:
        executor = SubprocessExecutor(max_memory_mb=512, timeout_s=60)
        await executor.start()
        result = await executor.compute_indicator(indicator, ctx)
        await executor.stop()

    Or as async context manager:
        async with SubprocessExecutor() as executor:
            result = await executor.compute_indicator(indicator, ctx)
    """

    def __init__(
        self,
        max_memory_mb: int = 2048,
        timeout_s: int = 60,
        max_workers: int = 4,
        cpp_pool=None,
    ) -> None:
        self._max_memory_mb = max_memory_mb
        self._timeout_s = timeout_s
        self._max_workers = max_workers
        self._semaphore: asyncio.Semaphore | None = None
        self.cpp_pool = cpp_pool

    async def start(self) -> None:
        self._semaphore = asyncio.Semaphore(self._max_workers)
        if self.cpp_pool is not None:
            await self.cpp_pool.start()

    async def stop(self) -> None:
        if self.cpp_pool is not None:
            await self.cpp_pool.stop()

    async def compute_indicator(
        self,
        indicator: Indicator,
        ctx: IndicatorContext,
    ) -> pl.Series:
        from tradingkit.indicator import CppIndicator
        if isinstance(indicator, CppIndicator):
            if self.cpp_pool is None:
                raise RuntimeError(
                    "CppIndicator requires CppRunnerPool — pass cpp_pool= to SubprocessExecutor"
                )
            arr = await self.cpp_pool.run(indicator._so_bytes, ctx.df, indicator.params)
            return pl.Series("value", arr, dtype=pl.Float64)

        payload = {
            "task": "indicator",
            "indicator": pickle.dumps(indicator),
            "data": _df_to_arrow_bytes(ctx.df),
        }
        result = await self._run_in_subprocess(payload)
        if "error" in result:
            raise RuntimeError(f"Subprocess indicator error: {result['error']}")
        arr = np.frombuffer(result["values"], dtype=np.float64)
        return pl.Series("value", arr, dtype=pl.Float64)

    async def process_strategy_bar(
        self,
        strategy: Strategy,
        bar: BarContext,
    ) -> Signal | None:
        payload = {
            "task": "strategy",
            "strategy": pickle.dumps(strategy),
            "bar": pickle.dumps(bar),
        }
        result = await self._run_in_subprocess(payload)
        if "error" in result:
            raise RuntimeError(f"Subprocess strategy error: {result['error']}")
        if result.get("signal") is None:
            return None
        from tradingkit.strategy import Signal
        return Signal.from_dict(result["signal"])

    async def fetch_source_data(
        self,
        source: DataSource,
        symbol: str,
        timeframe: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
    ) -> pl.DataFrame:
        payload = {
            "task": "source",
            "source": pickle.dumps(source),
            "symbol": symbol,
            "timeframe": timeframe,
            "start_ts": start_ts,
            "end_ts": end_ts,
            "limit": limit,
        }
        result = await self._run_in_subprocess(payload)
        if "error" in result:
            raise RuntimeError(f"Subprocess source error: {result['error']}")
        return _arrow_bytes_to_df(result["data"])

    async def run_strategy_script(
        self, code: str, candles: list[dict], params: dict | None = None, name: str = "strategy",
    ) -> list[dict]:
        result = await self._run_in_subprocess({
            "task": "strategy_script", "code": code, "candles": candles,
            "params": params, "name": name,
        })
        if "error" in result:
            raise RuntimeError(result["error"])
        return result["signals"]

    async def call(
        self,
        target: str,
        kwargs: dict | None = None,
        on_request: Callable[[dict], Awaitable[Any]] | None = None,
        network: bool = False,
    ) -> Any:
        """Run a trusted function, `module:function`, in a worker and return its plain
        result. Plugin code it executes stays out of this process; `on_request` answers
        the data requests the function makes through its `host` argument."""
        result = await self._run_in_subprocess(
            {"task": "call", "target": target, "kwargs": kwargs or {}},
            on_request=on_request, network=network,
        )
        if "error" in result:
            raise RuntimeError(result["error"])
        return result["value"]

    async def open_sink(self, sink: Sink) -> SinkSession:
        from tradingkit.executor.source_worker import WorkerSinkSession
        return WorkerSinkSession(sink, lambda: self._open("session", network=True))

    async def open_source(self, source: DataSource) -> SourceSession:
        from tradingkit.executor.source_worker import WorkerSourceSession
        return WorkerSourceSession(source, lambda: self._open("session", network=True))

    async def _open(self, worker: str, network: bool = False) -> Channel:
        """A fresh worker process: "task" runs one payload, "session" holds a plugin."""
        from tradingkit.executor.source_worker import worker_env
        if worker == "task":
            args = [f"--memory-mb={self._max_memory_mb}", f"--cpu-s={self._timeout_s}"]
            return await spawn_local(_WORKER_SCRIPT, args)
        return await spawn_local(_SESSION_SCRIPT, [f"--memory-mb={self._max_memory_mb}"], worker_env())

    async def _run_in_subprocess(
        self,
        payload: dict,
        on_request: Callable[[dict], Awaitable[Any]] | None = None,
        network: bool = False,
    ) -> dict:
        sem = self._semaphore or asyncio.Semaphore(self._max_workers)
        async with sem:
            channel = await self._open("task", network=network or payload.get("task") == "source")
            try:
                return await asyncio.wait_for(
                    self._exchange(channel, payload, on_request), timeout=self._timeout_s,
                )
            finally:
                await channel.close()

    @staticmethod
    async def _exchange(
        channel: Channel, payload: dict, on_request: Callable[[dict], Awaitable[Any]] | None,
    ) -> dict:
        await channel.send(payload)
        while True:
            try:
                message = await channel.receive()
            except asyncio.IncompleteReadError as exc:
                raise RuntimeError("Worker exited without a result") from exc
            if not isinstance(message, dict):
                raise TypeError("Worker sent a malformed message")
            if "request" in message:
                if on_request is None:
                    await channel.send({"error": "This task cannot request data"})
                    continue
                try:
                    await channel.send({"response": await on_request(message["request"])})
                except Exception as exc:
                    await channel.send({"error": str(exc)})
                continue
            result = message.get("result")
            if not isinstance(result, dict):
                raise TypeError("Worker sent a malformed result")
            return result
