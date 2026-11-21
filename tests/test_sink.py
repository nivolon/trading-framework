"""tradingkit.sink — the Sink contract."""
from __future__ import annotations

import json

import pytest
import websockets
from aiohttp import web
from aiohttp.test_utils import TestServer

from tradingkit.executor import LocalExecutor, SubprocessExecutor
from tradingkit.sink import ScriptSink, Sink


class Flaky(Sink):
    def __init__(self, failures, **config):
        super().__init__({"retry_delay_s": 0, "max_retries": 3, **config})
        self.failures = failures
        self.sent = []
        self.setups = 0
        self.closes = 0

    async def setup(self):
        self.setups += 1

    async def close(self):
        self.closes += 1

    async def send(self, event):
        if len(self.sent) + self.closes < self.failures:
            raise OSError("broken pipe")
        self.sent.append(event)


async def test_a_failed_send_is_retried_on_a_restored_connection():
    sink = Flaky(failures=2)
    await sink.deliver({"id": "1"})
    assert sink.sent == [{"id": "1"}]
    assert sink.closes == 2 and sink.setups == 3


async def test_retries_are_bounded_and_the_error_surfaces():
    sink = Flaky(failures=99)
    with pytest.raises(OSError):
        await sink.deliver({"id": "1"})
    assert sink.setups == 3


async def test_setup_runs_once_when_nothing_fails():
    sink = Flaky(failures=0)
    await sink.deliver({"id": "1"})
    await sink.deliver({"id": "2"})
    assert sink.setups == 1 and sink.closes == 0


async def test_a_failing_setup_is_retried():
    class Offline(Flaky):
        async def setup(self):
            self.setups += 1
            if self.setups == 1:
                raise OSError("refused")

    sink = Offline(failures=0)
    await sink.deliver({"id": "1"})
    assert sink.sent == [{"id": "1"}]


async def test_a_script_gets_its_merged_config():
    code = """
__params__ = {"limit": {"type": "int", "default": 1}}

class S(Sink):
    async def send(self, event):
        event["limit"] = self.config["limit"]
"""
    event = {}
    await ScriptSink(code, {"limit": "5"}).deliver(event)
    assert event == {"limit": 5}


async def test_a_script_without_a_class_is_rejected():
    with pytest.raises(ValueError, match="Sink subclass"):
        await ScriptSink("x = 1").deliver({})


async def test_local_session_keeps_one_instance():
    sink = Flaky(failures=0)
    session = await LocalExecutor().open_sink(sink)
    await session.deliver({"id": "1"})
    await session.close()
    assert sink.setups == 1 and sink.closes == 1


async def test_worker_keeps_a_connection_between_deliveries():
    code = """
import os

class S(Sink):
    async def setup(self):
        self.setups = getattr(self, "setups", 0) + 1

    async def send(self, event):
        if self.setups != 1 or event["n"] > 1 and os.getpid() != self.pid:
            raise ValueError("connection was not kept")
        self.pid = os.getpid()
"""
    session = await SubprocessExecutor().open_sink(ScriptSink(code))
    await session.deliver({"n": 1})
    await session.deliver({"n": 2})
    await session.close()


async def test_worker_surfaces_delivery_errors():
    code = "class S(Sink):\n    async def send(self, event):\n        raise KeyError('boom')"
    session = await SubprocessExecutor().open_sink(ScriptSink(code, {"max_retries": 1}))
    with pytest.raises(RuntimeError, match="boom"):
        await session.deliver({})
    await session.close()


# ------------------------------------------------------------------ #
# Built-in plugins against real servers                                #
# ------------------------------------------------------------------ #

def _template(name):
    import importlib.util
    import pathlib
    path = pathlib.Path(__file__).parents[2] / "plugins" / "sinks" / "loader.py"
    if not path.exists():
        pytest.skip("host app templates not available")
    spec = importlib.util.spec_from_file_location("sink_templates", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.BUILTIN_SINKS[name]


async def test_webhook_posts_the_event_with_its_token():
    received = []

    async def handle(request):
        received.append((await request.json(), request.headers.get("Authorization")))
        return web.Response()

    app = web.Application()
    app.router.add_post("/hook", handle)
    server = TestServer(app)
    await server.start_server()
    sink = ScriptSink(_template("webhook.py"), {"url": str(server.make_url("/hook")), "token": "t"})
    await sink.deliver({"id": "1"})
    await sink.shutdown()
    await server.close()
    assert received == [({"id": "1"}, "Bearer t")]


async def test_webhook_raises_on_an_error_status():
    async def handle(request):
        return web.Response(status=500)

    app = web.Application()
    app.router.add_post("/hook", handle)
    server = TestServer(app)
    await server.start_server()
    sink = ScriptSink(_template("webhook.py"), {
        "url": str(server.make_url("/hook")), "max_retries": 2, "retry_delay_s": 0,
    })
    with pytest.raises(Exception, match="500"):
        await sink.deliver({"id": "1"})
    await sink.shutdown()
    await server.close()


async def test_websocket_reconnects_after_the_server_drops_it():
    received, connections = [], []

    async def handler(socket, *_):
        connections.append(socket)
        received.append(json.loads(await socket.recv()))
        if len(connections) == 1:
            await socket.close()

    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    sink = ScriptSink(_template("websocket.py"), {
        "url": f"ws://127.0.0.1:{port}", "retry_delay_s": 0,
    })
    await sink.deliver({"id": "1"})
    import asyncio
    await asyncio.sleep(0.05)
    await sink.deliver({"id": "2"})
    await asyncio.sleep(0.05)
    await sink.shutdown()
    server.close()
    await server.wait_closed()
    assert [e["id"] for e in received] == ["1", "2"]
    assert len(connections) == 2
