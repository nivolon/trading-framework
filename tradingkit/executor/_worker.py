"""
tradingkit.executor._worker — one-shot worker process.

Reads one framed task, runs it and writes one framed result. While it runs, the task can
ask the host for data through HostLink.request(), which is how plugin code reaches data
without database credentials of its own.
"""
from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import pickle
import struct
import sys

_HEADER = struct.Struct(">I")


class HostLink:
    """Requests from a task to the host that started it."""

    def __init__(self, inp, out) -> None:
        self._in = inp
        self._out = out

    def _read_exact(self, size: int) -> bytes:
        chunks, remaining = [], size
        while remaining:
            chunk = self._in.read(remaining)
            if not chunk:
                raise EOFError("Host closed the channel")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    def read_message(self):
        size = _HEADER.unpack(self._read_exact(_HEADER.size))[0]
        return pickle.loads(self._read_exact(size))

    def send(self, message) -> None:
        from tradingkit.executor._wire import plain_frame
        self._out.write(plain_frame(message))
        self._out.flush()

    def request(self, kind: str, **args):
        self.send({"request": {"kind": kind, **args}})
        reply = self.read_message()
        if "error" in reply:
            raise RuntimeError(reply["error"])
        return reply["response"]


def _arrow_to_df(raw: bytes):
    import polars as pl
    import pyarrow as pa
    return pl.from_arrow(pa.ipc.open_stream(pa.py_buffer(raw)).read_all())


def _df_to_arrow(df) -> bytes:
    import pyarrow as pa
    table = df.to_arrow()
    sink = pa.BufferOutputStream()
    writer = pa.ipc.new_stream(sink, table.schema)
    writer.write_table(table)
    writer.close()
    return sink.getvalue().to_pybytes()


async def _run_indicator(payload: dict, host: HostLink) -> dict:
    import numpy as np

    from tradingkit.indicator import IndicatorContext
    indicator = pickle.loads(payload["indicator"])
    series = indicator(IndicatorContext(_arrow_to_df(payload["data"])))
    return {"values": np.asarray(series.to_numpy(), dtype=np.float64).tobytes()}


async def _run_strategy(payload: dict, host: HostLink) -> dict:
    strategy = pickle.loads(payload["strategy"])
    signal = await strategy.on_bar(pickle.loads(payload["bar"]))
    return {"signal": signal.to_dict() if signal is not None else None}


async def _run_strategy_script(payload: dict, host: HostLink) -> dict:
    from tradingkit.strategy import run_strategy_script
    return {"signals": run_strategy_script(
        payload["code"], payload["candles"], payload["params"], payload["name"],
    )}


async def _run_source(payload: dict, host: HostLink) -> dict:
    source = pickle.loads(payload["source"])
    df = await source.get_historical_data(
        symbol=payload["symbol"], timeframe=payload["timeframe"],
        start_ts=payload["start_ts"], end_ts=payload["end_ts"], limit=payload["limit"],
    )
    return {"data": _df_to_arrow(df)}


async def _run_call(payload: dict, host: HostLink) -> dict:
    """Call a host-chosen function, `module:function`, with plain keyword arguments."""
    module_name, _, func_name = payload["target"].partition(":")
    func = getattr(importlib.import_module(module_name), func_name)
    kwargs = dict(payload.get("kwargs") or {})
    if "host" in inspect.signature(func).parameters:
        kwargs["host"] = host
    result = func(**kwargs)
    if inspect.isawaitable(result):
        result = await result
    return {"value": result}


_TASKS = {
    "indicator": _run_indicator,
    "strategy": _run_strategy,
    "strategy_script": _run_strategy_script,
    "source": _run_source,
    "call": _run_call,
}


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--memory-mb", type=int, default=512)
    parser.add_argument("--cpu-s", type=int, default=0)
    args = parser.parse_args()

    from tradingkit.executor._wire import apply_limits, worker_stdio
    apply_limits(args.memory_mb, args.cpu_s or None)
    inp, out = worker_stdio()
    host = HostLink(inp, out)

    try:
        payload = host.read_message()
    except EOFError:
        sys.exit(1)
    try:
        task = _TASKS.get(payload.get("task"))
        if task is None:
            raise ValueError(f"Unknown task: {payload.get('task')!r}")
        result = await task(payload, host)
    except Exception as exc:
        result = {"error": str(exc)}
    host.send({"result": result})


if __name__ == "__main__":
    asyncio.run(main())
