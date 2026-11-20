"""
tradingkit.formula — an expression over named series, for `FORMULA` in an aggregation
script: a synthetic market written as one line instead of a combine() function.

    FORMULA = "se3 - dk1"                 # a spread
    FORMULA = "avg(dk1, dk2)"             # a basket
    FORMULA = "zscore(se3 - dk1, 96)"     # how far the spread is from its last 96 bars
    FORMULA = "dk1.high - dk1.low"        # a column other than the close

A name is a series, matched case-insensitively; alone it means its close, `name.column`
any other column. The series are aligned on their timestamps and the formula is computed
row by row, never executed as code: it is parsed and only the operators and functions
below are accepted.

    + - * / **, numbers
    abs(x) sqrt(x) log(x) exp(x)
    min(a, b, ...) max(a, b, ...) avg(a, b, ...)       across series, per row
    mean(x, n) std(x, n) zscore(x, n)                  over the last n rows
    change(x, n) pct(x, n) lag(x, n)                   against n rows back
"""
from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field

import polars as pl

MAX_LENGTH = 500
MAX_WINDOW = 100_000
_NAME_RE = re.compile(r"[a-z][a-z0-9_]{0,31}")
_UNARY = {"abs", "sqrt", "log", "exp"}
_ACROSS = {"min", "max", "avg"}
_WINDOWED = {"mean", "std", "zscore", "change", "pct", "lag"}
FUNCTIONS = sorted(_UNARY | _ACROSS | _WINDOWED)


@dataclass
class Formula:
    text: str
    tree: ast.expr
    #: Series the formula reads, in the order they first appear.
    names: list[str] = field(default_factory=list)
    #: Rows needed before the first one computed, for its windows.
    warmup: int = 0


def _column(name: str, column: str) -> str:
    return f"{name}__{column}"


def parse(text: str) -> Formula:
    """Check a formula and read its names and warmup; raises ValueError saying what is wrong."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("The formula is empty")
    if len(text) > MAX_LENGTH:
        raise ValueError(f"The formula is longer than {MAX_LENGTH} characters")
    try:
        tree = ast.parse(text.strip(), mode="eval").body
    except SyntaxError as e:
        raise ValueError(f"The formula cannot be read near character {e.offset or 0}: {e.msg}") from None
    formula = Formula(text=text.strip(), tree=tree)
    _check(tree, formula)
    if not formula.names:
        raise ValueError("The formula names no series")
    return formula


def _name(raw: str, formula: Formula) -> str:
    name = raw.lower()
    if name in FUNCTIONS:
        raise ValueError(f"{raw} is a function: call it as {raw}(...)")
    if not _NAME_RE.fullmatch(name):
        raise ValueError(f"{raw!r}: a series name is letters, digits and _, starting with a letter")
    if name not in formula.names:
        formula.names.append(name)
    return name


def _check(node: ast.AST, formula: Formula) -> None:
    if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow)):
        _check(node.left, formula)
        _check(node.right, formula)
    elif isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        _check(node.operand, formula)
    elif isinstance(node, ast.Constant) and isinstance(node.value, (int, float)) and not isinstance(node.value, bool):
        pass
    elif isinstance(node, ast.Name):
        _name(node.id, formula)
    elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
        _name(node.value.id, formula)
        if not _NAME_RE.fullmatch(node.attr.lower()):
            raise ValueError(f"{node.attr!r} is not a column name")
    elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and not node.keywords:
        func = node.func.id.lower()
        if func not in FUNCTIONS:
            raise ValueError(f"No function {node.func.id}; the formula knows {', '.join(FUNCTIONS)}")
        if func in _UNARY and len(node.args) != 1:
            raise ValueError(f"{func}(x) takes one value")
        if func in _ACROSS and not node.args:
            raise ValueError(f"{func}(...) needs at least one value")
        if func in _WINDOWED:
            if len(node.args) != 2:
                raise ValueError(f"{func}(x, n) takes a value and a number of rows")
            window = node.args[1]
            if not (isinstance(window, ast.Constant) and isinstance(window.value, int)
                    and not isinstance(window.value, bool) and 1 <= window.value <= MAX_WINDOW):
                raise ValueError(f"{func}(x, n): n is a whole number of rows from 1 to {MAX_WINDOW}")
            formula.warmup = max(formula.warmup, window.value)
            _check(node.args[0], formula)
        else:
            for arg in node.args:
                _check(arg, formula)
    else:
        raise ValueError(f"The formula cannot use {ast.unparse(node)!r}")


def _expr(node: ast.AST) -> pl.Expr:
    if isinstance(node, ast.BinOp):
        left, right = _expr(node.left), _expr(node.right)
        if isinstance(node.op, ast.Add):
            return left + right
        if isinstance(node.op, ast.Sub):
            return left - right
        if isinstance(node.op, ast.Mult):
            return left * right
        if isinstance(node.op, ast.Div):
            return left / right
        return left ** right
    if isinstance(node, ast.UnaryOp):
        return -_expr(node.operand) if isinstance(node.op, ast.USub) else _expr(node.operand)
    if isinstance(node, ast.Constant):
        return pl.lit(float(node.value))
    if isinstance(node, ast.Name):
        return pl.col(_column(node.id.lower(), "close"))
    if isinstance(node, ast.Attribute):
        return pl.col(_column(node.value.id.lower(), node.attr.lower()))
    func = node.func.id.lower()
    args = node.args
    if func in _UNARY:
        x = _expr(args[0])
        return {"abs": x.abs(), "sqrt": x.sqrt(), "log": x.log(), "exp": x.exp()}[func]
    if func in _ACROSS:
        values = [_expr(arg) for arg in args]
        return {"min": pl.min_horizontal, "max": pl.max_horizontal, "avg": pl.mean_horizontal}[func](values)
    x, n = _expr(args[0]), args[1].value
    if func == "mean":
        return x.rolling_mean(n)
    if func == "std":
        return x.rolling_std(n)
    if func == "zscore":
        return (x - x.rolling_mean(n)) / x.rolling_std(n)
    if func == "change":
        return x - x.shift(n)
    if func == "pct":
        return x / x.shift(n) - 1
    return x.shift(n)


def evaluate(formula: Formula, series: dict[str, pl.DataFrame]) -> pl.DataFrame:
    """The formula over series aligned on timestamp: a frame of `timestamp` and `value`,
    only the rows where every series has a row and the value is a finite number."""
    joined: pl.DataFrame | None = None
    for name in formula.names:
        df = series.get(name)
        if df is None or df.is_empty() or "timestamp" not in df.columns:
            return pl.DataFrame(schema={"timestamp": pl.Int64, "value": pl.Float64})
        df = df.unique("timestamp", keep="last").sort("timestamp")
        renamed = df.select(
            pl.col("timestamp").cast(pl.Int64),
            *[pl.col(c).cast(pl.Float64, strict=False).alias(_column(name, c)) for c in df.columns if c != "timestamp"
              and df[c].dtype.is_numeric()],
        )
        joined = renamed if joined is None else joined.join(renamed, on="timestamp", how="inner")
    assert joined is not None
    needed = {_column(n.value.id.lower(), n.attr.lower()) for n in ast.walk(formula.tree) if isinstance(n, ast.Attribute)}
    needed |= {_column(n.id.lower(), "close") for n in ast.walk(formula.tree)
               if isinstance(n, ast.Name) and n.id.lower() not in FUNCTIONS and _is_series(formula.tree, n)}
    missing = sorted(c.replace("__", ".") for c in needed if c not in joined.columns)
    if missing:
        raise ValueError(f"No such column: {', '.join(missing)}")
    out = joined.sort("timestamp").select("timestamp", _expr(formula.tree).alias("value"))
    return out.filter(pl.col("value").is_finite())


def _is_series(tree: ast.AST, name: ast.Name) -> bool:
    """A bare name that is a series, not the value inside `name.column` or a function."""
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute) and node.value is name:
            return False
        if isinstance(node, ast.Call) and node.func is name:
            return False
    return True


__all__ = ["FUNCTIONS", "Formula", "evaluate", "parse"]
