"""Worker isolation: output decoding, host data requests and the sandbox spawner."""
from __future__ import annotations

import os
import pickle
import subprocess
import sys
import textwrap
import time

import numpy as np
import polars as pl
import pytest

from tradingkit.executor import SandboxExecutor, SubprocessExecutor
from tradingkit.executor._wire import UnsafeFrameError, plain, plain_loads
from tradingkit.indicator import IndicatorContext, ScriptIndicator
from tradingkit.source import ScriptSource


class _Boom:
    def __reduce__(self):
        return (os.getcwd, ())


def test_plain_loads_accepts_builtin_values():
    value = {"a": [1, 2.5, "x", b"y", None, True], "b": {"c": (1, 2)}}
    assert plain_loads(pickle.dumps(value)) == {"a": [1, 2.5, "x", b"y", None, True], "b": {"c": (1, 2)}}


def test_plain_loads_never_calls_into_a_module():
    with pytest.raises(UnsafeFrameError, match="getcwd"):
        plain_loads(pickle.dumps(_Boom()))


def test_plain_converts_library_values():
    import datetime as dt
    out = plain({"n": np.float64(1.5), "arr": np.array([1, 2]), "when": dt.date(2026, 1, 2), "obj": object})
    assert out["n"] == 1.5 and out["arr"] == [1, 2] and out["when"] == "2026-01-02"
    assert isinstance(out["obj"], str)
    plain_loads(pickle.dumps(out))


_FORGE = """
import os, pickle, struct
class _R:
    def __reduce__(self):
        return (open, ({marker!r}, "w"))
raw = pickle.dumps({{"result": _R()}})
for fd in range(3, 64):
    try:
        os.write(fd, struct.pack(">I", len(raw)) + raw)
    except OSError:
        pass
result = close
"""


async def test_a_worker_cannot_make_the_host_run_code(tmp_path):
    marker = tmp_path / "pwned"
    indicator = ScriptIndicator(code=_FORGE.format(marker=str(marker)))
    ctx = IndicatorContext(pl.DataFrame({"timestamp": [1, 2], "close": [1.0, 2.0]}))
    with pytest.raises((UnsafeFrameError, RuntimeError)):
        await SubprocessExecutor(timeout_s=30).compute_indicator(indicator, ctx)
    assert not marker.exists()


@pytest.fixture
def helper_module(tmp_path, monkeypatch):
    (tmp_path / "sandbox_helpers.py").write_text(textwrap.dedent("""
        import os, time

        def double_rows(host, n):
            rows = host.request("rows", n=n)
            return [r * 2 for r in rows]

        def environment():
            return {"password": os.getenv("DATABASE_PASSWORD"), "uid": os.getuid()}

        def spin():
            while True:
                pass
    """))
    monkeypatch.setenv("PYTHONPATH", f"{tmp_path}{os.pathsep}{os.getenv('PYTHONPATH', '')}")
    return tmp_path


async def test_call_answers_data_requests_from_the_worker(helper_module):
    async def serve(request):
        assert request == {"kind": "rows", "n": 3}
        return [1, 2, 3]

    result = await SubprocessExecutor(timeout_s=30).call(
        "sandbox_helpers:double_rows", {"n": 3}, on_request=serve,
    )
    assert result == [2, 4, 6]


async def test_call_without_a_data_handler_reports_it(helper_module):
    with pytest.raises(RuntimeError, match="cannot request data"):
        await SubprocessExecutor(timeout_s=30).call("sandbox_helpers:double_rows", {"n": 1})


@pytest.fixture
def spawner(helper_module):
    import shutil
    import tempfile
    # Unix socket paths are limited to about a hundred bytes, which pytest's tmp_path exceeds.
    directory = tempfile.mkdtemp(prefix="sbx", dir="/tmp")
    path = os.path.join(directory, "s.sock")
    env = {**os.environ, "SANDBOX_PYTHONPATH": os.environ["PYTHONPATH"] + os.pathsep + os.pathsep.join(sys.path)}
    env.pop("DATABASE_PASSWORD", None)
    process = subprocess.Popen(
        [sys.executable, "-m", "tradingkit.executor._spawner", "--socket", path,
         "--max-task-s", "5", "--wall-grace-s", "1"],
        env=env,
    )
    deadline = time.monotonic() + 10
    while not os.path.exists(path):
        assert time.monotonic() < deadline, "spawner did not start"
        time.sleep(0.05)
    yield path
    process.terminate()
    process.wait(5)
    shutil.rmtree(directory, ignore_errors=True)


async def test_sandbox_runs_indicators_and_calls(spawner):
    executor = SandboxExecutor(compute_socket=spawner, timeout_s=30)
    ctx = IndicatorContext(pl.DataFrame({"timestamp": [1, 2, 3], "close": [1.0, 2.0, 3.0]}))
    values = await executor.compute_indicator(ScriptIndicator(code="result = close * 2"), ctx)
    assert values.to_list() == [2.0, 4.0, 6.0]

    async def serve(request):
        return [5]
    assert await executor.call("sandbox_helpers:double_rows", {"n": 1}, on_request=serve) == [10]


async def test_sandbox_workers_get_a_clean_environment(spawner, monkeypatch):
    monkeypatch.setenv("DATABASE_PASSWORD", "secret")
    env = await SandboxExecutor(compute_socket=spawner).call("sandbox_helpers:environment")
    assert env["password"] is None


async def test_sandbox_kills_a_task_past_its_budget(spawner):
    executor = SandboxExecutor(compute_socket=spawner, timeout_s=60)
    started = time.monotonic()
    with pytest.raises(RuntimeError):
        await executor.call("sandbox_helpers:spin")
    assert time.monotonic() - started < 30


async def test_sandbox_holds_source_sessions(spawner):
    code = """
import os

class S(DataSource):
    async def history(self, stream, symbol, start_ts, end_ts, limit):
        return [{"timestamp": start_ts, "pid": os.getpid()}]
"""
    session = await SandboxExecutor(compute_socket=spawner).open_source(ScriptSource(code))
    first = await session.fetch("BTC", 60, 0, 60)
    second = await session.fetch("BTC", 60, 0, 60)
    await session.close()
    assert first["pid"][0] == second["pid"][0]


def _ipc(df) -> bytes:
    import io
    buffer = io.BytesIO()
    df.write_ipc(buffer)
    return buffer.getvalue()


async def test_indicator_preview_runs_in_a_worker_and_queries_through_the_host():
    from tradingkit.indicator import preview_indicator_script  # noqa: F401
    candles = pl.DataFrame({"timestamp": [60, 120], "open": [1.0, 2.0], "high": [1.0, 2.0],
                            "low": [1.0, 2.0], "close": [1.0, 2.0], "volume": [1.0, 1.0]})
    asked = []

    async def serve(request):
        asked.append(request)
        return _ipc(candles.with_columns(pl.col("close") * 10))

    code = "other = ctx.query('ETH_USDT', '1m')\nresult = close + other['close'].to_numpy() * k"
    for executor in (SubprocessExecutor(timeout_s=60), __import__("tradingkit").executor.LocalExecutor()):
        result = await executor.call(
            "tradingkit.indicator:preview_indicator_script",
            {"code": code, "params": {"k": 2}, "data": _ipc(candles), "exchange": "whitebit"},
            on_request=serve,
        )
        assert result == {"type": "line", "data": [[60, 21.0], [120, 42.0]]}
    assert asked[0] == {"kind": "candles", "exchange": "whitebit", "symbol": "ETH_USDT", "timeframe": "1m"}
