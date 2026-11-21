"""
tradingkit.executor.sandbox — SandboxExecutor.

Runs every plugin in workers started by a sandbox spawner (tradingkit.executor._spawner)
in another container: no database credentials, no host files, a user of its own per
worker, and only the network that container has. Compute work (indicators, strategies,
aggregation specs) goes to one spawner, ideally with no network at all; sources and sinks,
which must reach the outside world, go to another.

    executor = SandboxExecutor(
        compute_socket="/run/sandbox/compute.sock",
        network_socket="/run/sandbox/network.sock",
    )

Worker output is decoded with _wire.plain_loads(), so a worker can return data but never
make this process construct an object.
"""
from __future__ import annotations

from tradingkit.executor._wire import Channel, spawn_sandboxed
from tradingkit.executor.subprocess_ import SubprocessExecutor


class SandboxExecutor(SubprocessExecutor):
    def __init__(self, compute_socket: str, network_socket: str | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self._compute_socket = compute_socket
        self._network_socket = network_socket or compute_socket

    async def start(self, wait_s: float = 30.0) -> None:
        """Wait for the spawners to come up, since they may start after this process."""
        import asyncio
        import os
        await super().start()
        loop = asyncio.get_running_loop()
        deadline = loop.time() + wait_s
        for path in {self._compute_socket, self._network_socket}:
            while not os.path.exists(path):
                if loop.time() > deadline:
                    raise RuntimeError(f"Sandbox spawner socket {path} did not appear")
                await asyncio.sleep(0.2)

    async def _open(self, worker: str, network: bool = False) -> Channel:
        header = {"worker": worker, "memory_mb": self._max_memory_mb}
        if worker == "task":
            header["cpu_s"] = self._timeout_s
        return await spawn_sandboxed(self._network_socket if network else self._compute_socket, header)


__all__ = ["SandboxExecutor"]
