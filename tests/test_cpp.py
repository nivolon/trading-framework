"""tradingkit.cpp — C++ indicators compiled and run in a worker."""
from __future__ import annotations

import shutil

import numpy as np
import pytest

from tradingkit.core.script_errors import ScriptError
from tradingkit.cpp import (
    check_includes,
    compile_indicator,
    cpp_param_schema,
    cpp_params,
    is_cpp,
    resolve_cpp_params,
    run_cpp_indicator,
)

SMA = '''
#include <cstdlib>
#include <cstring>

static int read_int(const char* json, const char* key, int fallback) {
    const char* p = json ? strstr(json, key) : nullptr;
    if (!p || !(p = strchr(p, ':'))) return fallback;
    return (int)strtol(p + 1, nullptr, 10);
}

static double read_double(const char* json, const char* key, double fallback) {
    const char* p = json ? strstr(json, key) : nullptr;
    if (!p || !(p = strchr(p, ':'))) return fallback;
    return strtod(p + 1, nullptr);
}

extern "C" void indicator_compute(
    const double* close, const double* high, const double* low,
    const double* open,  const double* volume,
    int length, double* result, const char* params_json)
{
    int period = read_int(params_json, "period", 3);
    double scale = read_double(params_json, "scale", 1.0);
    for (int i = period - 1; i < length; i++) {
        double sum = 0;
        for (int j = i - period + 1; j <= i; j++) sum += close[j];
        result[i] = scale * sum / period;
    }
}
'''

needs_compiler = pytest.mark.skipif(shutil.which("g++") is None, reason="no g++")


def _columns(close):
    close = np.asarray(close, dtype=np.float64)
    return {c: close for c in ("close", "high", "low", "open", "volume")}


def test_is_cpp_looks_for_the_entry_point():
    assert is_cpp(SMA)
    assert not is_cpp("result = close")


def test_params_and_their_defaults_come_from_the_code():
    assert cpp_params(SMA) == {"period": 3, "scale": 1.0}
    assert cpp_param_schema(SMA) == {
        "period": {"type": "int", "default": 3}, "scale": {"type": "float", "default": 1.0},
    }
    assert resolve_cpp_params(SMA, {"period": "5", "other": "x"}) == {"period": 5, "scale": 1.0, "other": "x"}
    with pytest.raises(ValueError, match="period"):
        resolve_cpp_params(SMA, {"period": "five"})


def test_only_allowed_headers_can_be_included():
    check_includes(SMA)
    with pytest.raises(ScriptError, match="<fstream> is not allowed") as e:
        check_includes("// header\n#include <fstream>\n")
    assert e.value.line == 2


@needs_compiler
def test_runs_with_parameters_and_leaves_warmup_bars_nan():
    values = run_cpp_indicator(SMA, _columns([1, 2, 3, 4, 5]), {"period": 2, "scale": 10.0})
    assert np.isnan(values[0])
    assert values[1:].tolist() == [15.0, 25.0, 35.0, 45.0]


@needs_compiler
def test_two_libraries_in_one_process_stay_apart():
    other = SMA.replace("scale * sum / period", "-1.0")
    first = run_cpp_indicator(SMA, _columns([1, 2, 3]), {"period": 1})
    second = run_cpp_indicator(other, _columns([1, 2, 3]), {"period": 1})
    assert first.tolist() == [1.0, 2.0, 3.0]
    assert second.tolist() == [-1.0, -1.0, -1.0]


@needs_compiler
def test_a_compile_error_names_its_line():
    broken = SMA.replace("double sum = 0;", "doubel sum = 0;")
    with pytest.raises(ScriptError) as e:
        compile_indicator(broken)
    assert e.value.line == broken.splitlines().index("        doubel sum = 0;") + 1
    assert str(e.value).startswith(f"Line {e.value.line}:")
