"""
tradingkit.core.plugin_runtime — what source and sink plugins share: one-time setup and an
HTTP session that lives as long as the plugin.
"""
from __future__ import annotations

import aiohttp


class PluginRuntime:
    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}
        self._http: aiohttp.ClientSession | None = None
        self._ready: bool = False

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        state["_http"] = None
        state["_ready"] = False
        return state

    async def setup(self) -> None:
        """Runs once, before the plugin is first used."""

    async def close(self) -> None:
        """Release what setup() acquired. The HTTP session is closed by the framework."""

    @property
    def session(self) -> aiohttp.ClientSession:
        """HTTP session shared by every call on this plugin."""
        if self._http is None or self._http.closed:
            timeout = float(self.config.get("request_timeout_s", 30))
            self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=timeout))
        return self._http

    async def open(self) -> None:
        if not self._ready:
            await self.setup()
            self._ready = True

    async def shutdown(self) -> None:
        if self._ready:
            await self.close()
            self._ready = False
        if self._http is not None:
            await self._http.close()
            self._http = None
