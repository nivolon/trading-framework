"""
tradingkit.executor._spawner — starts worker processes inside a sandbox.

Run it in a container that holds no credentials and reaches only the network its workers
may use. It listens on a Unix socket shared with the host application, and for every
connection starts one worker whose stdin and stdout are that connection:

    python -m tradingkit.executor._spawner --socket /run/sandbox/compute.sock \\
        --uid-first 20000 --uid-count 1000 --socket-gid 999

With --uid-first, each worker runs as its own user, so workers cannot signal, trace or
read each other, and every process a worker leaves behind is killed before its user is
reused. That needs root with CAP_SETUID, CAP_SETGID and CAP_KILL; without the option
workers run as the spawner's own user, which suits tests.

A connection opens with one JSON line, {"worker": "task"|"session", "memory_mb": N,
"cpu_s": N}, answered by "ok" or "error <reason>". Limits are capped by the spawner's
own options. A task is killed when its wall-clock budget runs out, and any worker is
killed when the host hangs up.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import select
import signal
import socket
import socketserver
import subprocess
import sys
import threading
import time

logger = logging.getLogger("tradingkit.spawner")

_HERE = os.path.dirname(os.path.abspath(__file__))
_SCRIPTS = {"task": "_worker.py", "session": "_source_worker.py"}
_HEADER_LIMIT = 4096
_POLL_MS = 500


class _NotStarted(Exception):
    pass


class UidPool:
    def __init__(self, first: int, count: int) -> None:
        self._free = list(range(first, first + count))
        self._lock = threading.Lock()

    def take(self) -> int:
        with self._lock:
            if not self._free:
                raise RuntimeError("no free worker user")
            return self._free.pop(0)

    def release(self, uid: int) -> None:
        with self._lock:
            self._free.append(uid)


def _read_header(conn: socket.socket) -> dict:
    data = b""
    while not data.endswith(b"\n"):
        chunk = conn.recv(1)
        if not chunk:
            raise ValueError("connection closed before the header")
        data += chunk
        if len(data) > _HEADER_LIMIT:
            raise ValueError("header too long")
    header = json.loads(data)
    if not isinstance(header, dict) or header.get("worker") not in _SCRIPTS:
        raise ValueError("unknown worker kind")
    return header


def _home(uid: int | None) -> str:
    return f"/tmp/worker-{uid}" if uid else "/tmp"


def _sweep_uid(uid: int) -> None:
    """Kill every process the worker user still owns and delete its files, before the
    user serves anyone else. kill(-1) from that user reaches exactly its own processes."""
    script = (
        "import os, shutil, signal\n"
        "try:\n"
        "    os.kill(-1, signal.SIGKILL)\n"
        "except ProcessLookupError:\n"
        "    pass\n"
        f"shutil.rmtree({_home(uid)!r})\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        user=uid, group=uid, extra_groups=[], env={}, cwd="/", check=False,
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )
    if result.returncode != 0:
        logger.error(f"could not clean up worker user {uid}: {result.stderr.strip()[-500:]}")


class Spawner:
    def __init__(self, opts: argparse.Namespace) -> None:
        self.opts = opts
        self.uids = UidPool(opts.uid_first, opts.uid_count) if opts.uid_first else None
        self.slots = threading.BoundedSemaphore(opts.max_workers)

    def _env(self, uid: int | None) -> dict[str, str]:
        threads = str(self.opts.threads)
        home = _home(uid)
        return {
            "PATH": "/usr/local/bin:/usr/bin:/bin",
            "PYTHONPATH": self.opts.pythonpath,
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONUNBUFFERED": "1",
            "HOME": home,
            "NUMBA_CACHE_DIR": f"{home}/numba",
            "MPLCONFIGDIR": f"{home}/mpl",
            "POLARS_MAX_THREADS": threads,
            "OPENBLAS_NUM_THREADS": threads,
            "OMP_NUM_THREADS": threads,
            "NUMBA_NUM_THREADS": threads,
            "TOKIO_WORKER_THREADS": threads,
            "TRADINGKIT_WORKER_FILE_MB": str(self.opts.max_file_mb),
            # RLIMIT_NPROC counts every process and thread of the user, so it bounds a worker
            # only under a uid of its own; under a shared one it counts the host's processes
            # too and leaves the worker unable to start a thread.
            **({"TRADINGKIT_WORKER_NPROC": str(self.opts.max_procs)} if uid is not None else {}),
        }

    def serve(self, conn: socket.socket) -> None:
        if not self.slots.acquire(blocking=False):
            conn.sendall(b"error busy\n")
            return
        uid = None
        try:
            try:
                header = _read_header(conn)
                uid = self.uids.take() if self.uids else None
            except (ValueError, RuntimeError) as exc:
                conn.sendall(f"error {exc}\n".encode())
                return
            self._run(conn, header, uid)
        except _NotStarted as exc:
            logger.error(f"worker failed to start: {exc}")
            conn.sendall(f"error worker failed to start: {exc}\n".encode())
        finally:
            if uid is not None:
                _sweep_uid(uid)
                self.uids.release(uid)
            self.slots.release()

    def _run(self, conn: socket.socket, header: dict, uid: int | None) -> None:
        opts = self.opts
        task = header["worker"] == "task"
        memory_mb = max(64, min(int(header.get("memory_mb") or opts.max_memory_mb), opts.max_memory_mb))
        args = [sys.executable, os.path.join(_HERE, _SCRIPTS[header["worker"]]), f"--memory-mb={memory_mb}"]
        deadline = None
        if task:
            cpu_s = max(1, min(int(header.get("cpu_s") or opts.max_task_s), opts.max_task_s))
            args.append(f"--cpu-s={cpu_s}")
            deadline = time.monotonic() + cpu_s + opts.wall_grace_s
        env = self._env(uid)
        try:
            if uid is not None:
                # The spawner holds no DAC override, so it needs search permission to start
                # the worker there; files inside stay private through the worker's umask.
                os.mkdir(env["HOME"], 0o711)
                os.chmod(env["HOME"], 0o711)
                os.chown(env["HOME"], uid, uid)
            process = subprocess.Popen(
                args, stdin=conn.fileno(), stdout=conn.fileno(), env=env, cwd=env["HOME"],
                close_fds=True, start_new_session=True, umask=0o077,
                **({"user": uid, "group": uid, "extra_groups": []} if uid is not None else {}),
            )
        except OSError as exc:
            raise _NotStarted(exc) from exc
        conn.sendall(b"ok\n")
        hangup = select.POLLHUP | select.POLLERR | getattr(select, "POLLRDHUP", 0)
        poller = select.poll()
        poller.register(conn.fileno(), hangup)
        try:
            while process.poll() is None:
                if poller.poll(_POLL_MS) or (deadline and time.monotonic() > deadline):
                    break
        finally:
            if process.poll() is None:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            process.wait()


class _Handler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        try:
            self.server.spawner.serve(self.request)
        except Exception:
            logger.exception("worker failed to start")


class _Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--socket", required=True)
    parser.add_argument("--socket-gid", type=int, default=None, help="group allowed to connect")
    parser.add_argument("--uid-first", type=int, default=0)
    parser.add_argument("--uid-count", type=int, default=1000)
    parser.add_argument("--max-workers", type=int, default=int(os.getenv("SANDBOX_MAX_WORKERS", "32")))
    parser.add_argument("--max-memory-mb", type=int, default=int(os.getenv("SANDBOX_MAX_MEMORY_MB", "2048")))
    parser.add_argument("--max-task-s", type=int, default=int(os.getenv("SANDBOX_MAX_TASK_S", "120")))
    parser.add_argument("--wall-grace-s", type=int, default=int(os.getenv("SANDBOX_WALL_GRACE_S", "30")))
    parser.add_argument("--max-procs", type=int, default=int(os.getenv("SANDBOX_MAX_PROCS", "64")),
                        help="processes and threads per worker user")
    parser.add_argument("--max-file-mb", type=int, default=int(os.getenv("SANDBOX_MAX_FILE_MB", "64")))
    parser.add_argument("--threads", type=int, default=int(os.getenv("SANDBOX_WORKER_THREADS", "1")))
    parser.add_argument("--pythonpath", default=os.getenv("SANDBOX_PYTHONPATH", os.getenv("PYTHONPATH", "")))
    opts = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    if os.path.exists(opts.socket):
        os.unlink(opts.socket)
    server = _Server(opts.socket, _Handler)
    server.spawner = Spawner(opts)
    os.chmod(opts.socket, 0o660 if opts.socket_gid is not None else 0o600)
    if opts.socket_gid is not None:
        os.chown(opts.socket, -1, opts.socket_gid)
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown).start())
    logger.info(f"sandbox spawner listening on {opts.socket}")
    server.serve_forever()


if __name__ == "__main__":
    main()
