"""simulate_trades — orders become trades the way an account would trade them."""
from __future__ import annotations

import pytest

from tradingkit.backtest import TradeRules, simulate_trades

MIN = 60


def bars(*ohlc):
    """Bars a minute apart from (open, high, low, close), or a close alone."""
    rows = []
    for i, values in enumerate(ohlc):
        o, h, lo, c = (values,) * 4 if isinstance(values, (int, float)) else values
        rows.append({"timestamp": i * MIN, "open": o, "high": h, "low": lo, "close": c})
    return rows


def test_an_order_fills_at_the_next_open_with_slippage_and_fees():
    data = bars((100, 100, 100, 100), (101, 103, 101, 102), (104, 106, 104, 105), (106, 106, 106, 106))
    report = simulate_trades(data, [(0, 1), (MIN, -1)], TradeRules(fee_pct=0.1, slippage_pct=0.5))
    (trade,) = report["trades"]
    assert trade["entry_price"] == pytest.approx(101 * 1.005)
    assert trade["exit_price"] == pytest.approx(104 * 0.995)
    qty = 10_000 / (101 * 1.005)
    expected = qty * (104 * 0.995 - 101 * 1.005) - 10 - qty * 104 * 0.995 * 0.001
    assert trade["pnl"] == pytest.approx(expected)
    assert report["stats"]["final_equity"] == pytest.approx(10_000 + expected)
    assert report["stats"]["fees"] == pytest.approx(10 + qty * 104 * 0.995 * 0.001)


def test_close_fill_trades_at_the_signal_bar_close():
    report = simulate_trades(bars(100, 110, 121), [(0, 1), (2 * MIN, -1)], TradeRules(fill="close"))
    (trade,) = report["trades"]
    assert (trade["entry_price"], trade["exit_price"]) == (100, 121)
    assert report["stats"]["net_return"] == pytest.approx(0.21)


def test_an_order_on_the_last_bar_has_nothing_to_fill_on():
    report = simulate_trades(bars(100, 101), [(MIN, 1)])
    assert report["trades"] == [] and report["open_trade"] is None


def test_long_only_ignores_a_sell_with_nothing_open_and_short_sells():
    data = bars(100, 100, 90, 90, 80)
    assert simulate_trades(data, [(0, -1), (2 * MIN, 1)], TradeRules(fill="close"))["trades"] == []
    report = simulate_trades(data, [(0, -1), (4 * MIN, 1)], TradeRules(direction="short", fill="close"))
    (trade,) = report["trades"]
    assert trade["side"] == "short" and trade["return"] == pytest.approx(0.2)


def test_both_directions_turn_around_on_each_signal():
    report = simulate_trades(bars(100, 110, 100, 90), [(0, 1), (MIN, -1), (3 * MIN, 1)],
                             TradeRules(direction="both", fill="close"))
    assert [t["side"] for t in report["trades"]] == ["long", "short"]
    assert report["open_trade"]["side"] == "long"


def test_an_exit_action_closes_without_opening():
    report = simulate_trades(bars(100, 110, 120), [(0, 1), (MIN, 0)], TradeRules(direction="both", fill="close"))
    assert len(report["trades"]) == 1 and report["open_trade"] is None


def test_a_stop_loss_exits_at_its_level_or_at_a_gap_open():
    data = bars((100, 100, 100, 100), (100, 101, 97, 99), (99, 99, 99, 99))
    (trade,) = simulate_trades(data, [(0, 1)], TradeRules(fill="close", stop_loss_pct=2))["trades"]
    assert trade["exit_price"] == pytest.approx(98) and trade["exit_reason"] == "stop_loss"
    gapped = bars((100, 100, 100, 100), (95, 96, 94, 95))
    (trade,) = simulate_trades(gapped, [(0, 1)], TradeRules(fill="close", stop_loss_pct=2))["trades"]
    assert trade["exit_price"] == 95


def test_a_bar_reaching_stop_and_target_counts_as_stopped():
    data = bars((100, 100, 100, 100), (100, 110, 90, 100))
    rules = TradeRules(fill="close", stop_loss_pct=5, take_profit_pct=5)
    (trade,) = simulate_trades(data, [(0, 1)], rules)["trades"]
    assert trade["exit_reason"] == "stop_loss"


def test_take_profit_and_a_short_stop():
    data = bars((100, 100, 100, 100), (100, 104, 99, 103), (103, 106, 102, 105))
    (trade,) = simulate_trades(data, [(0, 1)], TradeRules(fill="close", take_profit_pct=5))["trades"]
    assert trade["exit_price"] == pytest.approx(105) and trade["exit_reason"] == "take_profit"
    (trade,) = simulate_trades(data, [(0, -1)], TradeRules(direction="short", fill="close", stop_loss_pct=5))["trades"]
    assert trade["exit_price"] == pytest.approx(105) and trade["side"] == "short"


def test_a_trailing_stop_follows_the_best_price():
    data = bars((100, 100, 100, 100), (100, 120, 100, 119), (119, 119, 107, 110))
    (trade,) = simulate_trades(data, [(0, 1)], TradeRules(fill="close", trailing_stop_pct=10))["trades"]
    assert trade["exit_price"] == pytest.approx(108) and trade["exit_reason"] == "trailing_stop"


def test_max_bars_closes_at_the_close():
    (trade,) = simulate_trades(bars(100, 101, 102, 103, 104), [(0, 1)], TradeRules(fill="close", max_bars=2))["trades"]
    assert trade["exit_price"] == 102 and trade["bars"] == 2 and trade["exit_reason"] == "max_bars"
    (trade,) = simulate_trades(bars(100, 101, 102, 103, 104), [(0, 1)], TradeRules(max_bars=2))["trades"]
    assert (trade["entry_price"], trade["exit_price"]) == (101, 102)


def test_sizing_by_percent_amount_and_risk():
    data = bars(100, 100, 110)
    half = simulate_trades(data, [(0, 1)], TradeRules(fill="close", size=50))["open_trade"]
    assert half["qty"] == pytest.approx(50)
    fixed = simulate_trades(data, [(0, 1)], TradeRules(fill="close", sizing="amount", size=1000))["open_trade"]
    assert fixed["qty"] == pytest.approx(10)
    risk = simulate_trades(data, [(0, 1)], TradeRules(fill="close", sizing="risk", size=1, stop_loss_pct=4))["open_trade"]
    assert risk["qty"] == pytest.approx(10_000 * 0.01 / 0.04 / 100)


def test_stats_drawdown_win_rate_and_months():
    data = [{"timestamp": ts, "open": c, "high": c, "low": c, "close": c}
            for ts, c in [(1_704_067_200, 100), (1_704_153_600, 120), (1_706_745_600, 90), (1_706_832_000, 99)]]
    report = simulate_trades(data, [(1_704_067_200, 1), (1_704_153_600, -1), (1_706_745_600, 1), (1_706_832_000, -1)],
                             TradeRules(fill="close"))
    stats = report["stats"]
    assert stats["trades"] == 2 and stats["win_rate"] == 1.0
    assert stats["net_return"] == pytest.approx(1.2 * 1.1 - 1)
    assert stats["max_drawdown"] == 0
    assert stats["buy_and_hold"] == pytest.approx(-0.01)
    assert [m["month"] for m in report["monthly"]] == ["2024-01", "2024-02"]
    assert report["monthly"][0]["return"] == pytest.approx(0.2)
    assert report["monthly"][1]["return"] == pytest.approx(0.1)


def test_the_curve_is_thinned_but_keeps_the_last_point():
    report = simulate_trades(bars(*range(100, 200)), [], TradeRules(curve_points=10))
    assert len(report["curve"]) <= 11 and report["curve"][-1][0] == 99 * MIN


@pytest.mark.parametrize("values, message", [
    ({"direction": "sideways"}, "direction"),
    ({"sizing": "risk"}, "needs a stop-loss"),
    ({"size": 150}, "100 %"),
    ({"stop_loss_pct": -1}, "above zero"),
    ({"fee_pct": "abc"}, "not a number"),
    ({"leverage": 2}, "Unknown trade rule"),
])
def test_rules_refuse_what_cannot_trade(values, message):
    with pytest.raises(ValueError, match=message):
        TradeRules.from_dict(values)


def test_rules_from_a_form_treat_empty_as_none():
    rules = TradeRules.from_dict({"fee_pct": "0.1", "stop_loss_pct": "", "max_bars": "5", "direction": "both"})
    assert (rules.fee_pct, rules.stop_loss_pct, rules.max_bars, rules.direction) == (0.1, None, 5, "both")
