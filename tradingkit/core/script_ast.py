"""
tradingkit.core.script_ast — ScriptAST: parse-without-exec introspection shared by
AggregationScript (tradingkit.aggregation) and ConnectionScriptSource (tradingkit.source).

Both classes need the same two primitives on a plugin script string, without ever
exec()-ing it: "does it define function X at module level?" and "what's the literal
value assigned to module-level name Y?". This was previously duplicated across both
classes (and duplicated a second time *within* AggregationScript as near-identical
_ast_str/_ast_literal methods) — written once here, with the parsed tree cached the
way AggregationScript already did.
"""
from __future__ import annotations

import ast
from typing import Any


class ScriptAST:
    """Cached, exec-free AST introspection for a plugin script string."""

    def __init__(self, code: str) -> None:
        self._code = code
        self._tree: ast.Module | None = None

    def _get_tree(self) -> ast.Module:
        if self._tree is None:
            self._tree = ast.parse(self._code)
        return self._tree

    def has_function(self, name: str) -> bool:
        """True if `def name(...)` or `async def name(...)` is defined anywhere in the script."""
        try:
            return any(
                isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name
                for n in ast.walk(self._get_tree())
            )
        except Exception:
            return False

    def has_class(self, base: str | None = None) -> bool:
        """True if the script defines a class -- optionally, one listing `base` among its
        bases. Used to tell a class-style plugin script apart from a function-style or
        expression-style one without executing it."""
        try:
            for node in ast.walk(self._get_tree()):
                if not isinstance(node, ast.ClassDef):
                    continue
                if base is None:
                    return True
                for b in node.bases:
                    name = b.id if isinstance(b, ast.Name) else (
                        b.attr if isinstance(b, ast.Attribute) else None
                    )
                    if name == base:
                        return True
        except Exception:
            pass
        return False

    def get_literal(self, var_name: str) -> Any | None:
        """ast.literal_eval() of a `var_name = <literal>` module-level assignment, or None."""
        try:
            for node in ast.walk(self._get_tree()):
                if (
                    isinstance(node, ast.Assign)
                    and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name)
                    and node.targets[0].id == var_name
                ):
                    return ast.literal_eval(node.value)
        except Exception:
            pass
        return None


_PARAM_TYPES = {bool: "bool", int: "int", float: "float", str: "str"}


def script_params(code: str) -> dict[str, dict[str, Any]]:
    """The `__params__` a script declares, normalised to `{name: {"type", "default", ...}}`
    and read without executing it. A bare `"period": 14` is shorthand for a default, with
    the type taken from that value."""
    declared = ScriptAST(code).get_literal("__params__")
    if not isinstance(declared, dict):
        return {}
    params: dict[str, dict[str, Any]] = {}
    for name, spec in declared.items():
        entry = dict(spec) if isinstance(spec, dict) else {"default": spec}
        entry.setdefault("type", _PARAM_TYPES.get(type(entry.get("default")), "str"))
        params[name] = entry
    return params


def _cast_param(name: str, value: Any, spec: dict[str, Any]) -> Any:
    kind = spec["type"]
    try:
        if kind == "bool":
            if isinstance(value, str) and value.lower() in ("true", "false"):
                result: Any = value.lower() == "true"
            elif isinstance(value, bool):
                result = value
            else:
                raise ValueError
        elif kind == "int":
            if isinstance(value, bool) or not float(value).is_integer():
                raise ValueError
            result = int(float(value))
        elif kind == "float":
            if isinstance(value, bool):
                raise ValueError
            result = float(value)
        else:
            result = value
    except (TypeError, ValueError):
        raise ValueError(f"{name}: expected {kind}, got {value!r}") from None

    if "options" in spec and result not in spec["options"]:
        raise ValueError(f"{name}: {result!r} is not one of {spec['options']}")
    if "min" in spec and result < spec["min"]:
        raise ValueError(f"{name}: {result!r} is below the minimum {spec['min']}")
    if "max" in spec and result > spec["max"]:
        raise ValueError(f"{name}: {result!r} is above the maximum {spec['max']}")
    return result


def resolve_params(code: str, overrides: dict[str, Any] | None = None) -> dict[str, Any]:
    """Values a script runs with: its `__params__` defaults, then `overrides` cast to the
    declared type. Keeps code a template -- the values live with whoever runs it. Raises
    ValueError on a value that doesn't fit; names not declared pass through untouched."""
    schema = script_params(code)
    values = {name: spec["default"] for name, spec in schema.items() if "default" in spec}
    for name, value in (overrides or {}).items():
        values[name] = _cast_param(name, value, schema[name]) if name in schema else value
    return values


__all__ = ["ScriptAST", "resolve_params", "script_params"]
