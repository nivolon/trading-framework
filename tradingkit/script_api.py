"""
tradingkit.script_api — indicators and strategies written as one function, whose signature
says what it reads, what can be set and what it gives back:

    def custom_rsi(source: Series, period: int = 14) -> Series:
        return ta.rsi(source, timeperiod=period)

    from project.indicators import custom_rsi

    def strategy(bars: Bars, low: float = 30, high: float = 70) -> Signals:
        rsi = custom_rsi(bars.close, period=14)
        return Signals(buy=cross_below(rsi, low), sell=cross_above(rsi, high))

An argument typed `Series` is one series of the market, and which one (close, high, hl2...)
is a parameter the user picks; one typed `Bars` gets every column. Any other argument with
a default is a parameter. The script's function is its last public top-level function.

Other scripts are imported by name: `from project.indicators import custom_rsi` gives the
function of the project's custom_rsi.py, `from builtin.indicators import rsi` a built-in.
The caller that knows the project attaches what a script imports with `bundle()`, so the
code still travels as one string, and `run_script` installs it while the script runs.

Scripts written the older way (assign `result`, append to `signals`) still run as before.
"""
from __future__ import annotations

import ast
import contextlib
import importlib.abc
import importlib.util
import json
import sys
import types
from typing import Any

import numpy as np

#: The series a `Series` argument can be, as the user picks it.
COLUMNS = ("close", "open", "high", "low", "volume", "hl2", "hlc3", "ohlc4")

#: Packages a script imports other scripts from.
PROJECT_PACKAGES = {"indicators": "indicator", "strategies": "strategy"}

_BUNDLE_MARK = "# tradingkit:modules "


class Series(np.ndarray):
    """One value per bar. Only a type to write in a signature: values are numpy arrays."""


class Bars:
    """Every column of the bars a script runs over, as numpy arrays of the same length."""

    def __init__(self, columns: dict[str, np.ndarray]) -> None:
        self._columns = dict(columns)

    def __getattr__(self, name: str) -> np.ndarray:
        columns = self.__dict__.get("_columns", {})
        if name in columns:
            return columns[name]
        if name in ("hl2", "hlc3", "ohlc4"):
            return series_of(columns, name)
        raise AttributeError(f"bars has no column {name!r}; it has {', '.join(columns)}")

    def __getitem__(self, name: str) -> np.ndarray:
        return getattr(self, name)

    def __len__(self) -> int:
        return len(self._columns.get("close", ()))

    @property
    def time(self) -> np.ndarray:
        return self._columns["timestamp"]

    @property
    def columns(self) -> list[str]:
        return list(self._columns)


class Signals:
    """What a strategy gives back: on which bars to buy, sell or close, each a list of
    True/False per bar (or None for never).

        return Signals(buy=fast > slow, sell=fast < slow)
    """

    def __init__(self, buy: Any = None, sell: Any = None, exit: Any = None) -> None:
        self.when = {kind: when for kind, when in (("buy", buy), ("sell", sell), ("exit", exit)) if when is not None}

    def events(self, length: int) -> list[tuple[int, str]]:
        """(bar index, kind) of every signal, in time order."""
        found = []
        for kind, when in self.when.items():
            flags = np.asarray(when, dtype=bool)
            if flags.shape != (length,):
                raise ValueError(f"Signals({kind}=...) has {flags.size} values for {length} bars")
            found.extend((int(i), kind) for i in np.flatnonzero(flags))
        return sorted(found)


def cross_above(a: Any, b: Any) -> np.ndarray:
    """True on the bars where `a` goes from at or below `b` to above it."""
    a, b = _pair(a, b)
    out = np.zeros(len(a), dtype=bool)
    out[1:] = (a[1:] > b[1:]) & (a[:-1] <= b[:-1])
    return out


def cross_below(a: Any, b: Any) -> np.ndarray:
    """True on the bars where `a` goes from at or above `b` to below it."""
    a, b = _pair(a, b)
    out = np.zeros(len(a), dtype=bool)
    out[1:] = (a[1:] < b[1:]) & (a[:-1] >= b[:-1])
    return out


def _pair(a: Any, b: Any) -> tuple[np.ndarray, np.ndarray]:
    a = np.asarray(a, dtype=np.float64)
    b = np.broadcast_to(np.asarray(b, dtype=np.float64), a.shape)
    return a, b


def series_of(columns: dict[str, np.ndarray], name: str) -> np.ndarray:
    if name in columns:
        return columns[name]
    o, h, l, c = (columns[k] for k in ("open", "high", "low", "close"))
    if name == "hl2":
        return (h + l) / 2
    if name == "hlc3":
        return (h + l + c) / 3
    if name == "ohlc4":
        return (o + h + l + c) / 4
    raise ValueError(f"{name!r} is not a series of the bars; pick one of {', '.join(COLUMNS)}")


# ── Reading a script without running it ──────────────────────────────────────

def _tree(code: str) -> ast.Module | None:
    try:
        return ast.parse(code)
    except SyntaxError:
        return None


def _entry(tree: ast.Module) -> ast.FunctionDef | None:
    defs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and not n.name.startswith("_")]
    return defs[-1] if defs else None


def _writes_result(tree: ast.Module) -> bool:
    """An older script: it assigns `result` or calls something (signals.append) at the top."""
    for node in tree.body:
        targets = node.targets if isinstance(node, ast.Assign) else [node.target] if isinstance(node, (ast.AugAssign, ast.AnnAssign)) else []
        if any(isinstance(t, ast.Name) and t.id in ("result", "signals") for t in targets):
            return True
        if isinstance(node, (ast.For, ast.While, ast.If, ast.With)) or (
            isinstance(node, ast.Expr) and not isinstance(node.value, ast.Constant)
        ):
            return True
    return False


def is_function_script(code: str) -> bool:
    tree = _tree(code)
    return bool(tree and _entry(tree) and not _writes_result(tree))


def entry_name(code: str) -> str | None:
    tree = _tree(code)
    entry = _entry(tree) if tree else None
    return entry.name if entry else None


def _annotation(node: ast.expr | None) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return ""


_TYPE_NAMES = {"int": "int", "float": "float", "bool": "bool", "str": "str"}
_VALUE_TYPES = {bool: "bool", int: "int", float: "float", str: "str"}


def _arguments(entry: ast.FunctionDef) -> list[tuple[str, str, Any, bool]]:
    """(name, annotation, default, has_default) of each argument."""
    args = entry.args.posonlyargs + entry.args.args
    defaults = [None] * (len(args) - len(entry.args.defaults)) + list(entry.args.defaults)
    found = []
    for arg, default in zip(args, defaults):
        found.append((arg.arg, _annotation(arg.annotation), *_literal(default)))
    for arg, default in zip(entry.args.kwonlyargs, entry.args.kw_defaults):
        found.append((arg.arg, _annotation(arg.annotation), *_literal(default)))
    return found


def _literal(node: ast.expr | None) -> tuple[Any, bool]:
    if node is None:
        return None, False
    try:
        return ast.literal_eval(node), True
    except ValueError:
        return None, False


def function_params(code: str) -> dict[str, dict[str, Any]]:
    """The parameters a function script's signature declares, as `__params__` would."""
    tree = _tree(code)
    entry = _entry(tree) if tree else None
    if not entry:
        return {}
    params: dict[str, dict[str, Any]] = {}
    for name, annotation, default, has_default in _arguments(entry):
        if annotation == "Bars":
            continue
        if annotation == "Series":
            params[name] = {"type": "str", "default": default if has_default else "close",
                            "options": list(COLUMNS), "input": True}
            continue
        type_ = _TYPE_NAMES.get(annotation) or _VALUE_TYPES.get(type(default), "str")
        spec: dict[str, Any] = {"type": type_}
        if has_default:
            spec["default"] = default
        else:
            spec["required"] = True
        params[name] = spec
    return params


def describe(code: str) -> dict[str, Any] | None:
    """What the editor shows over a function script: its inputs, parameters and output."""
    tree = _tree(code)
    entry = _entry(tree) if tree else None
    if not entry or _writes_result(tree):
        return None
    inputs, params = [], []
    for name, annotation, default, has_default in _arguments(entry):
        if annotation in ("Series", "Bars"):
            inputs.append({"name": name, "kind": annotation.lower(), **({"default": default} if has_default else {})})
        else:
            params.append({"name": name, "type": _TYPE_NAMES.get(annotation) or _VALUE_TYPES.get(type(default), "any"),
                           **({"default": default} if has_default else {})})
    return {"name": entry.name, "inputs": inputs, "params": params,
            "output": _annotation(entry.returns) or "", "imports": project_imports(code)}


def project_imports(code: str) -> list[dict[str, str]]:
    """The scripts of the project a script imports: [{"type": "indicator", "name": "custom_rsi"}]."""
    tree = _tree(code)
    if not tree:
        return []
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            parts = node.module.split(".")
            if parts[0] == "project" and len(parts) == 2 and parts[1] in PROJECT_PACKAGES:
                found += [{"type": PROJECT_PACKAGES[parts[1]], "name": alias.name} for alias in node.names]
            elif parts[0] == "project" and len(parts) == 3 and parts[1] in PROJECT_PACKAGES:
                found.append({"type": PROJECT_PACKAGES[parts[1]], "name": parts[2]})
            elif node.module == "project":
                found += [{"type": PROJECT_PACKAGES[alias.name], "name": "*"} for alias in node.names if alias.name in PROJECT_PACKAGES]
    return found


# ── Imports ──────────────────────────────────────────────────────────────────

def bundle(code: str, modules: dict[str, str]) -> str:
    """`code` with the scripts it imports attached, keyed 'indicators.custom_rsi'. They go
    after it, so the script's own line numbers stay as written."""
    if not modules:
        return code
    return f"{code.rstrip()}\n{_BUNDLE_MARK}{json.dumps(modules)}\n"


def unbundle(code: str) -> tuple[str, dict[str, str]]:
    at = code.rfind(f"\n{_BUNDLE_MARK}")
    if at < 0:
        return code, {}
    try:
        modules = json.loads(code[at + len(_BUNDLE_MARK) + 1:])
    except ValueError:
        return code, {}
    return code[:at + 1], modules


def script_globals() -> dict[str, Any]:
    """What every script can use without importing it."""
    from tradingkit.indicator import _ta_ns
    import math
    return {
        "np": np, "ta": _ta_ns, "math": math, "Series": Series, "Bars": Bars, "Signals": Signals,
        "cross_above": cross_above, "cross_below": cross_below,
    }


def _chartword_module() -> types.ModuleType:
    module = types.ModuleType("chartword")
    module.__dict__.update(script_globals())
    module.series_of = series_of
    return module


class _BuiltinIndicators(types.ModuleType):
    """`from builtin.indicators import rsi`: a TA-Lib function that takes `period` too."""

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__"):
            raise AttributeError(name)
        from tradingkit.indicator import _ta_ns
        try:
            fn = getattr(_ta_ns, name)
        except AttributeError as exc:
            raise ImportError(f"builtin.indicators has no {name!r}") from exc

        def indicator(*args: Any, **kwargs: Any) -> Any:
            return fn(*(np.asarray(a, dtype=np.float64) for a in args), **kwargs)

        indicator.__name__ = name
        return indicator


class _Finder(importlib.abc.MetaPathFinder, importlib.abc.Loader):
    def __init__(self, sources: dict[str, str]) -> None:
        self.sources = sources

    def find_spec(self, fullname: str, path: Any = None, target: Any = None):
        root = fullname.split(".")[0]
        if root in ("project", "builtin", "chartword"):
            return importlib.util.spec_from_loader(fullname, self, is_package=fullname.count(".") < 2)
        return None

    def create_module(self, spec):
        name = spec.name
        if name == "chartword":
            return _chartword_module()
        if name == "builtin.indicators":
            return _BuiltinIndicators(name)
        if name in ("project", "builtin"):
            return types.ModuleType(name)
        parts = name.split(".")
        if parts[0] == "project" and len(parts) == 2:
            return _ProjectPackage(name, parts[1], self)
        if parts[0] == "project" and len(parts) == 3:
            return self.load_script(parts[1], parts[2])
        raise ImportError(f"No module named {name!r}")

    def exec_module(self, module) -> None:
        module.__path__ = getattr(module, "__path__", [])

    def load_script(self, package: str, name: str) -> types.ModuleType:
        key = f"{package}.{name}"
        if key not in self.sources:
            what = PROJECT_PACKAGES.get(package, package).rstrip("s")
            raise ImportError(f"The project has no {what} {name!r}")
        from tradingkit.core.script_errors import run_script
        module = types.ModuleType(f"project.{key}")
        module.__dict__.update(script_globals())
        run_script(self.sources[key], f"<{name}.py>", module.__dict__)
        return module


class _ProjectPackage(types.ModuleType):
    """`project.indicators`: each script of the project by its name, as its function."""

    def __init__(self, name: str, package: str, finder: _Finder) -> None:
        super().__init__(name)
        self.__path__ = []
        self._package = package
        self._finder = finder

    def __getattr__(self, name: str) -> Any:
        if name.startswith("__") or name.startswith("_"):
            raise AttributeError(name)
        key = f"{self._package}.{name}"
        if key not in self._finder.sources:
            raise AttributeError(f"The project has no {PROJECT_PACKAGES.get(self._package, self._package)} {name!r}")
        code = self._finder.sources[key]
        if not is_function_script(code):
            raise ImportError(
                f"{name}.py is written the older way (it assigns result); write it as a function to import it, "
                f"e.g. def {name}(source: Series, period: int = 14) -> Series")
        module = self._finder.load_script(self._package, name)
        value = getattr(module, entry_name(code))
        setattr(self, name, value)
        return value


_active: list[_Finder] = []


def imports_for(code: str):
    """The imports a script's run needs: the ones bundled with it, unless a script that
    imports it is already running and has them."""
    return contextlib.nullcontext() if _active else script_modules(unbundle(code)[1])


@contextlib.contextmanager
def script_modules(modules: dict[str, str]):
    """While a script runs: `project.*`, `builtin.*` and `chartword` import from `modules`."""
    saved = {k: v for k, v in sys.modules.items() if k.split(".")[0] in ("project", "builtin", "chartword")}
    for key in saved:
        del sys.modules[key]
    finder = _Finder(modules)
    sys.meta_path.insert(0, finder)
    _active.append(finder)
    try:
        yield
    finally:
        _active.remove(finder)
        sys.meta_path.remove(finder)
        for key in [k for k in sys.modules if k.split(".")[0] in ("project", "builtin", "chartword")]:
            del sys.modules[key]
        sys.modules.update(saved)


# ── Running a function script ────────────────────────────────────────────────

def _call(code: str, columns: dict[str, np.ndarray], params: dict[str, Any], namespace: dict[str, Any] | None = None) -> Any:
    with imports_for(code):
        return _call_entry(unbundle(code)[0], columns, params, namespace)


def _call_entry(code: str, columns: dict[str, np.ndarray], params: dict[str, Any], namespace: dict[str, Any] | None) -> Any:
    from tradingkit.core.script_errors import ScriptError, run_script
    from tradingkit.core.script_ast import resolve_params

    scope = {**script_globals(), **(namespace or {})}
    run_script(code, "<script>", scope)
    tree = _tree(code)
    entry = _entry(tree)
    fn = scope[entry.name]
    values = {**resolve_params(code), **(params or {})}
    bars = Bars(columns)
    kwargs = {}
    for name, annotation, default, has_default in _arguments(entry):
        if annotation == "Bars":
            kwargs[name] = bars
        elif annotation == "Series":
            kwargs[name] = series_of(columns, values.get(name, default if has_default else "close"))
        elif name in values:
            kwargs[name] = values[name]
        elif not has_default:
            raise ScriptError(f"{entry.name}() needs a value for {name}")
    try:
        return fn(**kwargs)
    except ScriptError:
        raise
    except Exception as exc:
        import traceback
        lines = [f.lineno for f in traceback.extract_tb(exc.__traceback__) if f.filename == "<script>"]
        line = lines[-1] if lines else None
        raise ScriptError(f"{'Line %d: ' % line if line else ''}{type(exc).__name__}: {exc}", line) from exc


def call_indicator(code: str, columns: dict[str, np.ndarray], params: dict[str, Any],
                   namespace: dict[str, Any] | None = None) -> Any:
    """The series a function indicator gives: an array, or a dict of named arrays."""
    result = _call(code, columns, params, namespace)
    if isinstance(result, dict):
        return {k: np.asarray(v, dtype=np.float64) for k, v in result.items()}
    if isinstance(result, tuple):
        return np.asarray(result[0], dtype=np.float64)
    return np.asarray(result, dtype=np.float64)


def call_strategy(code: str, columns: dict[str, np.ndarray], params: dict[str, Any]) -> list[tuple[int, str]]:
    """(bar index, 'buy' | 'sell' | 'exit') of every signal a function strategy gives."""
    result = _call(code, columns, params)
    length = len(columns["close"])
    if result is None:
        return []
    if isinstance(result, Signals):
        return result.events(length)
    values = np.asarray(result, dtype=np.float64)
    return [(int(i), "buy" if values[i] > 0 else "sell") for i in np.flatnonzero(np.nan_to_num(values))]


__all__ = [
    "Bars", "COLUMNS", "Series", "Signals", "bundle", "call_indicator", "call_strategy",
    "cross_above", "cross_below", "describe", "entry_name", "function_params", "imports_for",
    "is_function_script", "project_imports", "script_globals", "script_modules", "unbundle",
]
