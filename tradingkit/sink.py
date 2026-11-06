"""
tradingkit.sink — Sink and ScriptSink: sending results wherever a user needs them.

A sink implements one delivery (send). A long-lived connection such as a WebSocket or a
FIX session opens in setup() and closes in close(). The framework owns retries, and
reconnects by running close() and setup() again after a failed send.

Usage:
    from tradingkit import Sink
    from tradingkit.sink import ScriptSink
"""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

from tradingkit.core.plugin_runtime import PluginRuntime
from tradingkit.core.script_ast import resolve_params

logger = logging.getLogger(__name__)


class Sink(PluginRuntime, ABC):
    """
    Base class for outputs.

    Implement send(). Raise on failure: the framework retries `max_retries` times,
    waiting `retry_delay_s` and doubling it, and restores the connection in between.
    An event carries a unique `id`, so a receiver can drop a repeated delivery.
    """

    @abstractmethod
    async def send(self, event: dict) -> None:
        """One attempt to deliver one event."""
        ...

    async def _restore_connection(self) -> None:
        try:
            await self.close()
        except Exception as exc:
            logger.debug(f"Sink close failed: {exc!r}")
        self._ready = False
        await self.open()

    async def deliver(self, event: dict) -> None:
        """send() with setup, retries and reconnecting applied."""
        attempts = int(self.config.get("max_retries", 5))
        delay = float(self.config.get("retry_delay_s", 1))
        for attempt in range(attempts):
            try:
                await self.open()
                await self.send(event)
                return
            except Exception as exc:
                if attempt == attempts - 1:
                    raise
                logger.warning(f"Delivery failed ({exc!r}), retrying")
                await asyncio.sleep(delay * 2 ** attempt)
                try:
                    await self._restore_connection()
                except Exception as reconnect_error:
                    logger.warning(f"Reconnect failed ({reconnect_error!r})")


class ScriptSink(Sink):
    """A sink written as a script: code defining one Sink subclass. The namespace provides
    Sink. `self.config` is __params__ defaults merged with the given config."""

    def __init__(self, code: str, config: dict | None = None) -> None:
        super().__init__(config)
        self._code = code
        self._impl: Sink | None = None

    def __getstate__(self) -> dict:
        state = super().__getstate__()
        state["_impl"] = None
        return state

    def _instance(self) -> Sink:
        if self._impl is None:
            namespace: dict[str, Any] = {"Sink": Sink}
            exec(compile(self._code, "<sink_script>", "exec"), namespace)  # noqa: S102
            classes = [
                v for v in namespace.values()
                if isinstance(v, type) and issubclass(v, Sink) and v is not Sink
            ]
            if not classes:
                raise ValueError("Sink script must define a Sink subclass")
            cls = max(classes, key=lambda c: len(c.__mro__))
            self._impl = cls(resolve_params(self._code, self.config))
        return self._impl

    async def send(self, event: dict) -> None:
        await self._instance().send(event)

    async def open(self) -> None:
        await self._instance().open()

    async def shutdown(self) -> None:
        if self._impl is not None:
            await self._impl.shutdown()

    async def deliver(self, event: dict) -> None:
        await self._instance().deliver(event)


__all__ = ["ScriptSink", "Sink"]
