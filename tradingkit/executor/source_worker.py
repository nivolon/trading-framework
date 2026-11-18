"""
tradingkit.executor.source_worker — plugin sessions backed by a long-lived process.

If the process dies it is started again on the next call, and an open stream resumes.
"""
from __future__ import annotations

import asyncio
import io
import logging
import os
import pickle
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TYPE_CHECKING, Any

import polars as pl

from tradingkit.executor._wire import Channel
from tradingkit.executor.base import SourceSession

if TYPE_CHECKING:
    from tradingkit.source import DataSource

logger = logging.getLogger(__name__)

_DEAD = object()
_RESPAWN_DELAY_S = 1.0
_STOP_TIMEOUT_S = 5.0


def worker_env() -> dict[str, str]:
    """Plugins do I/O, not number crunching, and every library sizing its thread pool to
    the machine exhausts the worker's address-space limit."""
    threads = os.getenv("TRADINGKIT_SOURCE_WORKER_THREADS", "1")
    return {
        **os.environ,
        "POLARS_MAX_THREADS": threads,
        "OPENBLAS_NUM_THREADS": threads,
        "OMP_NUM_THREADS": threads,
    }


class WorkerSourceSession(SourceSession):
    def __init__(self, source: DataSource, launch: Callable[[], Awaitable[Channel]]) -> None:
        super().__init__(source)
        self._launch = launch
        self._channel: Channel | None = None
        self._reader: asyncio.Task | None = None
        self._next_id = 0
        self._channels: dict[int, asyncio.Queue] = {}
        self._start_lock = asyncio.Lock()
        self._closed = False

    async def _ensure_started(self) -> None:
        async with self._start_lock:
            if self._channel is not None and self._channel.alive:
                return
            if self._channel is not None:
                await self._channel.close()
            self._channel = await self._launch()
            self._reader = asyncio.create_task(self._read_replies(self._channel))
            await self._request("init", source=pickle.dumps(self._source))

    async def _read_replies(self, worker: Channel) -> None:
        try:
            while True:
                message = await worker.receive()
                if not isinstance(message, dict):
                    raise TypeError("Worker sent a malformed message")
                channel = self._channels.get(message.get("id"))
                if channel is not None:
                    channel.put_nowait(message)
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        except Exception as exc:
            logger.error(f"Dropping a worker that broke the protocol: {exc}")
        await worker.close()
        for channel in self._channels.values():
            channel.put_nowait(_DEAD)

    def _send(self, **message: Any) -> None:
        from tradingkit.executor._wire import frame
        self._channel.write(frame(pickle.dumps(message)))

    def _open_channel(self) -> tuple[int, asyncio.Queue]:
        self._next_id += 1
        channel: asyncio.Queue = asyncio.Queue()
        self._channels[self._next_id] = channel
        return self._next_id, channel

    async def _request(self, op: str, **fields: Any) -> Any:
        request_id, channel = self._open_channel()
        try:
            self._send(op=op, id=request_id, **fields)
            await self._channel.drain()
            reply = await channel.get()
        finally:
            del self._channels[request_id]
        if reply is _DEAD:
            raise RuntimeError("Source worker exited")
        if "error" in reply:
            raise RuntimeError(reply["error"])
        return reply["result"]

    async def fetch(
        self,
        symbol: str,
        timeframe: int,
        start_ts: int,
        end_ts: int,
        limit: int = 5000,
        stream: str | None = None,
    ) -> pl.DataFrame:
        await self._ensure_started()
        raw = await self._request(
            "fetch", symbol=symbol, timeframe=timeframe,
            start_ts=start_ts, end_ts=end_ts, limit=limit, stream=stream,
        )
        return pl.read_ipc(io.BytesIO(raw))

    async def batch_gaps(self, gaps: list[dict]) -> list[dict]:
        await self._ensure_started()
        return await self._request("batch_gaps", gaps=gaps)

    async def stream(
        self,
        symbol: str,
        timeframe: int,
        on_event: Callable[[dict], None] | None = None,
        stream: str | None = None,
    ) -> AsyncIterator[dict]:
        emit = on_event or (lambda event: None)
        restarted = False
        while not self._closed:
            await self._ensure_started()
            request_id, channel = self._open_channel()
            try:
                self._send(
                    op="stream", id=request_id, symbol=symbol, timeframe=timeframe, stream=stream,
                )
                while True:
                    message = await channel.get()
                    if message is _DEAD:
                        break
                    if "row" in message:
                        if restarted:
                            restarted = False
                            emit({"event": "connected"})
                        yield message["row"]
                    elif "event" in message:
                        emit(message["event"])
                    elif "error" in message:
                        raise RuntimeError(message["error"])
                    else:
                        return
            finally:
                del self._channels[request_id]
                if self._channel is not None and self._channel.alive:
                    self._send(op="cancel", target=request_id)

            restarted = True
            emit({"event": "reconnecting", "error": "source worker exited"})
            await asyncio.sleep(_RESPAWN_DELAY_S)

    async def close(self) -> None:
        self._closed = True
        if self._channel is None or not self._channel.alive:
            return
        self._send(op="stop")
        if self._reader is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._reader), _STOP_TIMEOUT_S)
            except TimeoutError:
                logger.warning("Source worker did not stop, killing it")
        await self._channel.close()
        if self._reader is not None:
            await self._reader


class WorkerSinkSession(WorkerSourceSession):
    async def deliver(self, event: dict) -> None:
        await self._ensure_started()
        await self._request("deliver", event=event)
