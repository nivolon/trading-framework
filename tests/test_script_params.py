"""tradingkit.core.script_ast — __params__ parsing and value resolution."""
from __future__ import annotations

import pytest

from tradingkit.core.script_ast import resolve_params, script_params
from tradingkit.indicator import _extract_defaults

NESTED = '''
__params__ = {
    "period":  {"type": "int",   "default": 14, "min": 2, "max": 500},
    "level":   {"type": "float", "default": 30.0},
    "mode":    {"type": "str",   "default": "fast", "options": ["fast", "slow"]},
    "enabled": {"type": "bool",  "default": True},
}
'''


def test_nested_declaration_yields_every_default():
    # Regression: a regex stopped at the first "}", so nested declarations yielded nothing.
    expected = {"period": 14, "level": 30.0, "mode": "fast", "enabled": True}
    assert resolve_params(NESTED) == expected
    assert _extract_defaults(NESTED) == expected


def test_script_params_does_not_execute_the_script():
    code = 'raise SystemExit("executed")\n__params__ = {"period": 5}'
    assert script_params(code)["period"] == {"default": 5, "type": "int"}


@pytest.mark.parametrize("code", ["", "x = 1", "__params__ = make()", "def broken(:"])
def test_scripts_without_a_literal_declaration_have_no_params(code):
    assert script_params(code) == {}


def test_bare_value_infers_type_and_bool_is_not_int():
    params = script_params('__params__ = {"on": True, "n": 3, "x": 1.5, "s": "a"}')
    assert {k: v["type"] for k, v in params.items()} == {
        "on": "bool", "n": "int", "x": "float", "s": "str",
    }


def test_overrides_from_a_form_are_cast_to_the_declared_type():
    values = resolve_params(NESTED, {"period": "50", "level": "25.5", "enabled": "false"})
    assert values == {"period": 50, "level": 25.5, "mode": "fast", "enabled": False}


def test_undeclared_names_pass_through():
    assert resolve_params("x = 1", {"period": 20}) == {"period": 20}


@pytest.mark.parametrize("override, message", [
    ({"period": "abc"},  "expected int"),
    ({"period": 14.5},   "expected int"),
    ({"period": True},   "expected int"),
    ({"period": 1},      "below the minimum"),
    ({"period": 501},    "above the maximum"),
    ({"level": ""},      "expected float"),
    ({"mode": "medium"}, "not one of"),
    ({"enabled": 1},     "expected bool"),
])
def test_bad_values_are_rejected_by_name(override, message):
    with pytest.raises(ValueError, match=message):
        resolve_params(NESTED, override)
