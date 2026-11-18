"""
tradingkit.executor._wire — framing between the host and worker processes.

The host pickles what it sends: the worker runs untrusted code anyway, so nothing it
could do with a payload is worse than what the code itself may do. The other direction
is the dangerous one. A worker's output is written by the same process the user's code
runs in, so the host decodes it with plain_loads(), which accepts only built-in values
(dicts, lists, strings, bytes, numbers, booleans, None) and never imports or constructs a
class. Workers convert their results with plain() before sending them.
"""
from __future__ import annotations

import asyncio
import datetime as _dt
import io
import json
import os
import pickle
import struct
from typing import Any

_HEADER = struct.Struct(">I")
_MAX_FRAME_ENV = "TRADINGKIT_MAX_FRAME_MB"
_DEFAULT_MAX_FRAME_MB = 256


def max_frame_bytes() -> int:
    return int(os.getenv(_MAX_FRAME_ENV, _DEFAULT_MAX_FRAME_MB)) * 1024 * 1024


class UnsafeFrameError(ValueError):
    """A worker sent something other than plain built-in values."""


class _PlainUnpickler(pickle.Unpickler):
    def find_class(self, module: str, name: str) -> Any:
        raise UnsafeFrameError(f"Worker output may not reference {module}.{name}")

    def persistent_load(self, pid: Any) -> Any:
        raise UnsafeFrameError("Worker output may not use persistent ids")


def plain_loads(raw: bytes) -> Any:
    return _PlainUnpickler(io.BytesIO(raw)).load()


def plain(value: Any) -> Any:
    """`value` as built-in types only, so plain_loads() accepts it."""
    if value is None or type(value) in (bool, int, float, str, bytes):
        return value
    for kind in (bool, int, float, str, bytes):
        if isinstance(value, kind):
            return kind(value)
    if isinstance(value, dict):
        return {plain(k) if isinstance(k, (int, float, str, bool)) else str(k): plain(v)
                for k, v in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [plain(v) for v in value]
    if isinstance(value, (_dt.datetime, _dt.date)):
        return value.isoformat()
    item = getattr(value, "item", None)
    if callable(item) and getattr(value, "ndim", None) == 0:
        return plain(item())
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        return plain(tolist())
    return str(value)


def frame(raw: bytes) -> bytes:
    return _HEADER.pack(len(raw)) + raw


def plain_frame(message: Any) -> bytes:
    return frame(pickle.dumps(plain(message), protocol=pickle.HIGHEST_PROTOCOL))


async def read_frame(reader: asyncio.StreamReader, limit: int | None = None) -> bytes:
    size = _HEADER.unpack(await reader.readexactly(_HEADER.size))[0]
    if size > (limit or max_frame_bytes()):
        raise UnsafeFrameError(f"Worker frame of {size} bytes exceeds the limit")
    return await reader.readexactly(size)


class Channel:
    """A byte stream to one worker process, wherever it runs."""

    def __init__(self, reader: asyncio.StreamReader, writer: Any, process: Any = None) -> None:
        self.reader = reader
        self._writer = writer
        self._process = process
        self._closed = False

    @property
    def alive(self) -> bool:
        if self._closed:
            return False
        if self._process is not None:
            return self._process.returncode is None
        return not self.reader.at_eof()

    def write(self, data: bytes) -> None:
        self._writer.write(data)

    async def drain(self) -> None:
        await self._writer.drain()

    async def send(self, message: Any) -> None:
        self.write(frame(pickle.dumps(message, protocol=pickle.HIGHEST_PROTOCOL)))
        await self.drain()

    async def receive(self) -> Any:
        return plain_loads(await read_frame(self.reader))

    async def wait_closed(self, timeout: float) -> bool:
        """Wait for the worker to exit on its own; False if it is still running."""
        try:
            if self._process is not None:
                await asyncio.wait_for(self._process.wait(), timeout)
            else:
                await asyncio.wait_for(self.reader.read(), timeout)
            return True
        except TimeoutError:
            return False

    async def close(self) -> None:
        """Stop the worker: kill a local process, hang up on a sandboxed one."""
        if self._closed:
            return
        self._closed = True
        if self._process is not None:
            if self._process.returncode is None:
                self._process.kill()
            await self._process.wait()
            return
        self._writer.close()
        try:
            await self._writer.wait_closed()
        except (ConnectionError, OSError):
            pass


async def spawn_local(script: str, args: list[str], env: dict[str, str] | None = None) -> Channel:
    import sys
    process = await asyncio.create_subprocess_exec(
        sys.executable, script, *args,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, env=env,
    )
    return Channel(process.stdout, process.stdin, process)


async def spawn_sandboxed(socket_path: str, header: dict) -> Channel:
    """Ask a sandbox spawner (tradingkit.executor._spawner) for a worker."""
    reader, writer = await asyncio.open_unix_connection(socket_path, limit=2 ** 20)
    writer.write(json.dumps(header).encode() + b"\n")
    await writer.drain()
    reply = await reader.readline()
    if not reply.startswith(b"ok"):
        writer.close()
        raise RuntimeError(f"Sandbox refused the worker: {reply.decode(errors='replace').strip()}")
    return Channel(reader, writer)


def worker_stdio() -> tuple[Any, Any]:
    """Protocol streams for a worker, with fd 1 pointed at stderr so a plugin's print()
    cannot corrupt the protocol."""
    import sys
    out = os.fdopen(os.dup(sys.stdout.fileno()), "wb")
    os.dup2(sys.stderr.fileno(), sys.stdout.fileno())
    return sys.stdin.buffer, out


def apply_limits(memory_mb: int | None = None, cpu_s: int | None = None) -> None:
    """Hard resource limits a worker applies to itself before it runs any plugin code.
    The sandbox spawner also sets process and file-size limits through the environment."""
    try:
        import resource
    except ImportError:
        return
    limits = [(resource.RLIMIT_CORE, 0)]
    for env, kind, scale in (
        ("TRADINGKIT_WORKER_NPROC", resource.RLIMIT_NPROC, 1),
        ("TRADINGKIT_WORKER_FILE_MB", resource.RLIMIT_FSIZE, 1024 * 1024),
    ):
        if os.getenv(env):
            limits.append((kind, int(os.environ[env]) * scale))
    if memory_mb:
        limits.append((resource.RLIMIT_AS, memory_mb * 1024 * 1024))
    if cpu_s:
        limits.append((resource.RLIMIT_CPU, cpu_s))
    for kind, value in limits:
        try:
            resource.setrlimit(kind, (value, value))
        except (ValueError, OSError):
            pass


__all__ = [
    "Channel", "UnsafeFrameError", "apply_limits", "frame", "max_frame_bytes", "plain",
    "plain_frame", "plain_loads", "read_frame", "spawn_local", "spawn_sandboxed", "worker_stdio",
]
