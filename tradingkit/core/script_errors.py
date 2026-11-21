"""tradingkit.core.script_errors — point a script's error at the line that raised it."""
from __future__ import annotations

import traceback


class ScriptError(RuntimeError):
    """An error raised by user script code, with the script line it came from."""

    def __init__(self, message: str, line: int | None = None) -> None:
        super().__init__(message)
        self.line = line


def run_script(code: str, filename: str, namespace: dict) -> None:
    """exec() a script, turning its errors into ScriptError with the script line. What the
    script imports from the project comes bundled with it (tradingkit.script_api)."""
    from tradingkit.script_api import imports_for, unbundle
    try:
        with imports_for(code):
            exec(compile(unbundle(code)[0], filename, "exec"), namespace)  # noqa: S102
    except SyntaxError as exc:
        raise ScriptError(f"Line {exc.lineno}: {exc.msg}", exc.lineno) from exc
    except Exception as exc:
        lines = [frame.lineno for frame in traceback.extract_tb(exc.__traceback__) if frame.filename == filename]
        line = lines[-1] if lines else None
        where = f"Line {line}: " if line else ""
        raise ScriptError(f"{where}{type(exc).__name__}: {exc}", line) from exc


__all__ = ["ScriptError", "run_script"]
