"""
tradingkit.executor._source_worker — long-lived process holding one source instance.

Speaks length-prefixed pickle over stdin/stdout with WorkerSourceSession, so the plugin
is set up once and its state survives between calls while user code stays out of the
main process.
"""
from __future__ import annotations

import argparse
import asyncio
import io
import os
import pickle
import struct
import sys


def _apply_memory_limit(mb: int) -> None:
    try:
        import resource
        limit = mb * 1024 * 1024
        resource.setrlimit(resource.RLIMIT_AS, (limit, limit))
    except (ImportError, ValueError):
        pass


class _Worker:
    def __init__(self, out) -> None:
        self._out = out
        self._source = None
        self._tasks: dict[int, asyncio.Task] = {}

    def _send(self, **message) -> None:
        raw = pickle.dumps(message)
        self._out.write(struct.pack(">I", len(raw)) + raw)
        self._out.flush()

    async def _init(self, msg: dict) -> None:
        self._source = pickle.loads(msg["source"])
        await self._source.open()
        self._send(id=msg["id"], result=True)

    async def _fetch(self, msg: dict) -> None:
        df = await self._source.get_historical_data(
            msg["symbol"], msg["timeframe"], msg["start_ts"], msg["end_ts"], msg["limit"],
            msg["stream"],
        )
        buffer = io.BytesIO()
        df.write_ipc(buffer)
        self._send(id=msg["id"], result=buffer.getvalue())

    async def _batch_gaps(self, msg: dict) -> None:
        self._send(id=msg["id"], result=self._source.batch_gaps(msg["gaps"]))

    async def _stream(self, msg: dict) -> None:
        on_event = lambda event: self._send(id=msg["id"], event=event)  # noqa: E731
        rows = self._source.stream(msg["symbol"], msg["timeframe"], on_event, msg["stream"])
        async for row in rows:
            self._send(id=msg["id"], row=row)
        self._send(id=msg["id"], result=True)

    async def _guarded(self, handler, msg: dict) -> None:
        try:
            await handler(msg)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._send(id=msg["id"], error=repr(exc))

    async def run(self, reader: asyncio.StreamReader) -> None:
        handlers = {
            "init": self._init, "fetch": self._fetch,
            "batch_gaps": self._batch_gaps, "stream": self._stream,
        }
        while True:
            try:
                size = struct.unpack(">I", await reader.readexactly(4))[0]
                msg = pickle.loads(await reader.readexactly(size))
            except asyncio.IncompleteReadError:
                break
            if msg["op"] == "stop":
                break
            if msg["op"] == "cancel":
                task = self._tasks.pop(msg["target"], None)
                if task:
                    task.cancel()
                continue
            self._tasks[msg["id"]] = asyncio.create_task(self._guarded(handlers[msg["op"]], msg))

        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        if self._source is not None:
            await self._source.shutdown()


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--memory-mb", type=int, default=2048)
    args = parser.parse_args()
    _apply_memory_limit(args.memory_mb)

    # A plugin's print() must not end up in the protocol stream.
    out = os.fdopen(os.dup(sys.stdout.fileno()), "wb")
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())

    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    await _Worker(out).run(reader)


if __name__ == "__main__":
    asyncio.run(main())
