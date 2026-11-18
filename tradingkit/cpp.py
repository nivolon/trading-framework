"""
tradingkit.cpp — C++ indicators, compiled and run inside an executor worker.

A C++ indicator is one source file that exports

    extern "C" void indicator_compute(
        const double* close, const double* high, const double* low,
        const double* open,  const double* volume,
        int length, double* result, const char* params_json)

`result` starts filled with NaN; `params_json` is the indicator's parameters as JSON.
Parameters are declared by reading them with a default, for example
`read_int(params_json, "period", 20)`: cpp_params() finds such calls, so the UI can
build a form for them.

The worker compiles with the system g++ and loads the library from memory, so nothing
needs to be executable on disk.
"""
from __future__ import annotations

import ctypes
import hashlib
import io
import json
import os
import re
import subprocess
import tempfile
from typing import Any

import numpy as np
import polars as pl

from tradingkit.core.script_errors import ScriptError

#: Standard headers a C++ indicator may include: maths, containers and strings, no I/O,
#: networking, filesystem or threads.
ALLOWED_INCLUDES: frozenset[str] = frozenset({
    "cmath", "complex", "valarray", "cfloat", "climits", "cstdint",
    "numeric", "random", "ratio",
    "algorithm", "functional", "iterator", "utility",
    "tuple", "optional", "variant", "type_traits", "limits",
    "array", "vector", "deque", "queue", "stack",
    "map", "unordered_map", "set", "unordered_set", "list", "span",
    "string", "string_view", "sstream", "cstring", "cstdlib",
    "memory", "new", "cassert", "stdexcept", "bitset", "chrono",
})

_ENTRY_RE = re.compile(r'extern\s+"C"\s+void\s+indicator_compute\s*\(')
_INCLUDE_RE = re.compile(r'^\s*#\s*include\s*[<"]([^>"]+)[>"]', re.MULTILINE)
_PARAM_RE = re.compile(r'\b\w+\s*\(\s*params_json\s*,\s*"(\w+)"\s*,\s*(-?\d+(?:\.\d+)?)\s*\)')
_GCC_RE = re.compile(r"^<stdin>:(\d+):(?:\d+:)?\s*(?:fatal )?error:\s*(.+)$", re.MULTILINE)
_COLUMNS = ("close", "high", "low", "open", "volume")

_compiled: dict[str, Any] = {}


def is_cpp(code: str) -> bool:
    """Whether `code` is a C++ indicator: it defines the indicator_compute entry point."""
    return bool(_ENTRY_RE.search(code or ""))


def check_includes(code: str) -> None:
    """Raise ScriptError for an #include outside ALLOWED_INCLUDES."""
    for match in _INCLUDE_RE.finditer(code):
        if match.group(1) not in ALLOWED_INCLUDES:
            line = code.count("\n", 0, match.start()) + 1
            raise ScriptError(
                f"Line {line}: <{match.group(1)}> is not allowed; C++ indicators may include only"
                f" standard maths, container and string headers", line,
            )


def cpp_params(code: str) -> dict[str, int | float]:
    """Parameters the code reads from params_json, with their defaults."""
    params: dict[str, int | float] = {}
    for name, default in _PARAM_RE.findall(code):
        params.setdefault(name, float(default) if "." in default else int(default))
    return params


def cpp_param_schema(code: str) -> dict[str, dict[str, Any]]:
    """cpp_params() in the `{name: {"type", "default"}}` form Python scripts use."""
    return {name: {"type": "float" if isinstance(default, float) else "int", "default": default}
            for name, default in cpp_params(code).items()}


def resolve_cpp_params(code: str, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Defaults, then `overrides` cast to the declared type. Raises ValueError on a value
    that doesn't fit; names the code doesn't read pass through untouched."""
    schema = cpp_param_schema(code)
    values: dict[str, Any] = {name: spec["default"] for name, spec in schema.items()}
    for name, value in (overrides or {}).items():
        if name not in schema:
            values[name] = value
            continue
        try:
            values[name] = float(value) if schema[name]["type"] == "float" else int(float(value))
        except (TypeError, ValueError) as e:
            raise ValueError(f"{name}: {value!r} is not a number") from e
    return values


def compile_indicator(code: str, std: str = "c++20", optimize: str = "-O2") -> bytes:
    """The shared library built from `code`; compile errors raise ScriptError with the line."""
    if not re.fullmatch(r"c\+\+\d{2}", std):
        raise ValueError(f"Invalid C++ standard {std!r}")
    if optimize not in ("-O0", "-O1", "-O2", "-O3", "-Os"):
        raise ValueError(f"Invalid optimisation flag {optimize!r}")
    check_includes(code)
    if not is_cpp(code):
        raise ScriptError('A C++ indicator must define extern "C" void indicator_compute(...)')
    with tempfile.TemporaryDirectory() as tmp:
        output = os.path.join(tmp, "indicator.so")
        try:
            proc = subprocess.run(
                ["g++", "-shared", "-fPIC", f"-std={std}", optimize, "-x", "c++", "-", "-o", output],
                input=code.encode(), capture_output=True, timeout=60, check=False, cwd=tmp,
            )
        except FileNotFoundError as e:
            raise RuntimeError("No C++ compiler (g++) on this server") from e
        if proc.returncode != 0:
            text = proc.stderr.decode(errors="replace")
            match = _GCC_RE.search(text)
            if match:
                line = int(match.group(1))
                raise ScriptError(f"Line {line}: {match.group(2).strip()}", line)
            raise ScriptError(text.strip()[:500] or "Compilation failed")
        with open(output, "rb") as f:
            return f.read()


def _load(library: bytes) -> Any:
    """Load a shared library from bytes. From memory where the OS allows it, since worker
    temp directories may forbid executing files."""
    if hasattr(os, "memfd_create"):
        # The descriptor stays open: dlopen() returns an already loaded library for a
        # path it has seen, so a reused descriptor number would load the previous one.
        fd = os.memfd_create("tradingkit-indicator")
        os.write(fd, library)
        return ctypes.CDLL(f"/proc/self/fd/{fd}")
    with tempfile.NamedTemporaryFile(suffix=".so", delete=False) as f:
        f.write(library)
    try:
        return ctypes.CDLL(f.name)
    finally:
        os.unlink(f.name)


def _entry(code: str, std: str, optimize: str) -> Any:
    key = hashlib.sha256(f"{std}\0{optimize}\0{code}".encode()).hexdigest()
    if key not in _compiled:
        fn = _load(compile_indicator(code, std, optimize)).indicator_compute
        pointer = ctypes.POINTER(ctypes.c_double)
        fn.argtypes = [pointer] * 5 + [ctypes.c_int, pointer, ctypes.c_char_p]
        fn.restype = None
        _compiled[key] = fn
    return _compiled[key]


def run_cpp_indicator(
    code: str, columns: dict[str, np.ndarray], params: dict | None = None,
    std: str = "c++20", optimize: str = "-O2",
) -> np.ndarray:
    """indicator_compute over OHLCV columns of equal length; NaN where it wrote nothing."""
    fn = _entry(code, std, optimize)
    length = len(columns["close"])
    arrays = [np.ascontiguousarray(columns[c], dtype=np.float64) for c in _COLUMNS]
    result = np.full(length, np.nan, dtype=np.float64)
    pointer = ctypes.POINTER(ctypes.c_double)
    fn(*(a.ctypes.data_as(pointer) for a in arrays), length, result.ctypes.data_as(pointer),
       json.dumps(params or {}).encode())
    return result


def preview_cpp_indicator(code: str, params: dict, data: bytes, std: str = "c++20", optimize: str = "-O2") -> dict:
    """Run a C++ indicator over candles (Arrow IPC bytes) and return what a chart draws.
    Meant to run in a worker: executor.call("tradingkit.cpp:preview_cpp_indicator")."""
    from tradingkit.indicator import _preview_result
    df = pl.read_ipc(io.BytesIO(data)) if data else pl.DataFrame()
    if not len(df):
        return {"type": "line", "data": []}
    columns = {c: df[c].to_numpy() for c in _COLUMNS}
    values = run_cpp_indicator(code, columns, {**cpp_params(code), **(params or {})}, std, optimize)
    return _preview_result(values, df["timestamp"].to_numpy())


__all__ = [
    "ALLOWED_INCLUDES",
    "check_includes",
    "compile_indicator",
    "cpp_param_schema",
    "cpp_params",
    "is_cpp",
    "preview_cpp_indicator",
    "resolve_cpp_params",
    "run_cpp_indicator",
]
