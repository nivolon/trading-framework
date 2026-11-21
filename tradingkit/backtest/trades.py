"""
tradingkit.backtest.trades — TradeRules, simulate_trades().

Trades a series of orders over bars the way an account would: each order fills at a price
the market offered (the next bar's open, or the signal bar's close), pays slippage and a
fee, sizes the position from the equity at hand, and stop-loss, take-profit, trailing-stop
and time exits close it within later bars.

Orders are plain (timestamp, action) pairs: 1 buys, -1 sells, 0 closes whatever is open.
What a strategy's signals mean is its author's vocabulary (see Signal), so turning them
into actions is the caller's choice, not this module's.

    rules = TradeRules(direction="long", fee_pct=0.1, slippage_pct=0.05, stop_loss_pct=2)
    report = simulate_trades(bars, [(ts, 1), (ts2, -1)], rules)
    report["stats"]["net_return"], report["trades"], report["curve"]
"""
from __future__ import annotations

import itertools
import math
import statistics
from dataclasses import asdict, dataclass, fields
from datetime import UTC, datetime
from typing import Any

DIRECTIONS = ("long", "short", "both")
FILLS = ("next_open", "close")
SIZINGS = ("percent", "amount", "risk")
_SECONDS_PER_YEAR = 365 * 24 * 3600


@dataclass
class TradeRules:
    """How orders become trades. Percentages are in percent: 0.1 means 0.1 %."""

    #: "long" buys and sells out, "short" sells short and buys back, "both" turns around.
    direction: str = "long"
    #: "next_open" fills at the open of the bar after the signal, "close" at its close.
    fill: str = "next_open"
    fee_pct: float = 0.0
    slippage_pct: float = 0.0
    initial_capital: float = 10_000.0
    #: "percent" of equity, a fixed "amount", or "risk": the percent of equity a stop-loss
    #: would lose, which needs stop_loss_pct.
    sizing: str = "percent"
    size: float = 100.0
    stop_loss_pct: float | None = None
    take_profit_pct: float | None = None
    trailing_stop_pct: float | None = None
    #: Close a trade at the close of its max_bars-th bar.
    max_bars: int | None = None
    #: The equity curve in the report is thinned to about this many points.
    curve_points: int = 2000

    def __post_init__(self) -> None:
        if self.direction not in DIRECTIONS:
            raise ValueError(f"direction must be one of {', '.join(DIRECTIONS)}")
        if self.fill not in FILLS:
            raise ValueError(f"fill must be one of {', '.join(FILLS)}")
        if self.sizing not in SIZINGS:
            raise ValueError(f"sizing must be one of {', '.join(SIZINGS)}")
        for name in ("fee_pct", "slippage_pct"):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} cannot be negative")
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be above zero")
        if self.size <= 0:
            raise ValueError("size must be above zero")
        if self.sizing == "percent" and self.size > 100:
            raise ValueError("A position cannot be more than 100 % of equity")
        for name in ("stop_loss_pct", "take_profit_pct", "trailing_stop_pct"):
            value = getattr(self, name)
            if value is not None and value <= 0:
                raise ValueError(f"{name} must be above zero, or empty for none")
        if self.stop_loss_pct is not None and self.stop_loss_pct >= 100:
            raise ValueError("stop_loss_pct must be below 100")
        if self.sizing == "risk" and self.stop_loss_pct is None:
            raise ValueError("Sizing by risk needs a stop-loss")
        if self.max_bars is not None and self.max_bars < 1:
            raise ValueError("max_bars must be at least 1, or empty for none")
        if self.curve_points < 2:
            raise ValueError("curve_points must be at least 2")

    @classmethod
    def from_dict(cls, values: dict[str, Any]) -> TradeRules:
        """Rules from loosely typed input such as a form: empty strings mean "none"."""
        known = {f.name: f for f in fields(cls)}
        unknown = sorted(set(values) - set(known))
        if unknown:
            raise ValueError(f"Unknown trade rule {', '.join(unknown)}")
        parsed: dict[str, Any] = {}
        for name, value in values.items():
            if value is None or value == "":
                if name in ("stop_loss_pct", "take_profit_pct", "trailing_stop_pct", "max_bars"):
                    parsed[name] = None
                continue
            kind = str(known[name].type)
            try:
                if name in ("max_bars", "curve_points"):
                    parsed[name] = int(value)
                elif "float" in kind:
                    parsed[name] = float(value)
                else:
                    parsed[name] = str(value)
            except (TypeError, ValueError):
                raise ValueError(f"{name}: {value!r} is not a number") from None
        return cls(**parsed)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class _Account:
    def __init__(self, rules: TradeRules) -> None:
        self.rules = rules
        self.cash = rules.initial_capital
        self.fees = 0.0
        self.side = 0
        self.qty = 0.0
        self.entry: dict[str, Any] | None = None
        self.best = 0.0
        self.trades: list[dict[str, Any]] = []

    def _price(self, price: float, side: int) -> float:
        slip = self.rules.slippage_pct / 100
        return price * (1 + slip) if side > 0 else price * (1 - slip)

    def equity(self, price: float) -> float:
        if not self.side or self.entry is None:
            return self.cash
        return self.cash + self.side * self.qty * (price - self.entry["price"])

    def open(self, side: int, bar: dict, index: int, price: float) -> None:
        rules = self.rules
        fill = self._price(price, side)
        if rules.sizing == "percent":
            notional = self.cash * rules.size / 100
        elif rules.sizing == "amount":
            notional = min(rules.size, self.cash)
        else:
            assert rules.stop_loss_pct is not None
            notional = min(self.cash * rules.size / rules.stop_loss_pct, self.cash)
        if notional <= 0 or fill <= 0:
            return
        fee = notional * rules.fee_pct / 100
        self.cash -= fee
        self.fees += fee
        self.side, self.qty = side, notional / fill
        self.best = fill
        self.entry = {"time": int(bar["timestamp"]), "price": fill, "index": index, "fee": fee, "notional": notional}

    def close(self, bar: dict, index: int, price: float, reason: str) -> None:
        if not self.side or self.entry is None:
            return
        fill = self._price(price, -self.side)
        fee = self.qty * fill * self.rules.fee_pct / 100
        pnl = self.side * self.qty * (fill - self.entry["price"])
        self.cash += pnl - fee
        self.fees += fee
        net = pnl - fee - self.entry["fee"]
        self.trades.append({
            "side": "long" if self.side > 0 else "short",
            "entry_time": self.entry["time"], "entry_price": self.entry["price"],
            "exit_time": int(bar["timestamp"]), "exit_price": fill,
            "qty": self.qty, "pnl": net, "return": net / self.entry["notional"],
            "bars": index - self.entry["index"], "exit_reason": reason,
        })
        self.side, self.qty, self.entry = 0, 0.0, None

    def apply(self, action: int, bar: dict, index: int, price: float) -> None:
        direction = self.rules.direction
        if action == 0:
            target = 0
        elif direction == "both":
            target = action
        elif direction == "long":
            target = 1 if action > 0 else 0
        else:
            target = -1 if action < 0 else 0
        if target == self.side:
            return
        if self.side:
            self.close(bar, index, price, "signal")
        if target:
            self.open(target, bar, index, price)

    def exit_levels(self, bar: dict, index: int) -> None:
        """Close the position if this bar reached its stop, trailing stop or target. A bar
        that reaches both is taken as stopped first, since the order within it is unknown."""
        if not self.side or self.entry is None:
            return
        rules, side, entry = self.rules, self.side, self.entry["price"]
        stop, reason = None, "stop_loss"
        if rules.stop_loss_pct is not None:
            stop = entry * (1 - side * rules.stop_loss_pct / 100)
        if rules.trailing_stop_pct is not None:
            trail = self.best * (1 - side * rules.trailing_stop_pct / 100)
            if stop is None or side * (trail - stop) > 0:
                stop, reason = trail, "trailing_stop"
        target = entry * (1 + side * rules.take_profit_pct / 100) if rules.take_profit_pct is not None else None
        o, h, lo = float(bar["open"]), float(bar["high"]), float(bar["low"])
        worst, best = (lo, h) if side > 0 else (h, lo)
        if stop is not None and side * (o - stop) <= 0:
            self.close(bar, index, o, reason)
        elif stop is not None and side * (worst - stop) <= 0:
            self.close(bar, index, stop, reason)
        elif target is not None and side * (o - target) >= 0:
            self.close(bar, index, o, "take_profit")
        elif target is not None and side * (best - target) >= 0:
            self.close(bar, index, target, "take_profit")
        else:
            self.best = max(self.best, h) if side > 0 else min(self.best, lo)


def simulate_trades(
    bars: list[dict], orders: list[tuple[int, int]], rules: TradeRules | None = None,
) -> dict[str, Any]:
    """Trade `orders` over oldest-first `bars` (timestamp, open, high, low, close).

    Returns {"stats", "trades", "open_trade", "curve", "monthly"}: closed trades with their
    exit reason, the trade still open at the end marked to the last close, the equity at
    bar closes thinned to rules.curve_points, and the return of each calendar month (UTC).
    An order on a bar that is not in `bars` is ignored; with fill "next_open", an order on
    the last bar has no bar to fill on and is left out.
    """
    rules = rules or TradeRules()
    account = _Account(rules)
    by_bar: dict[int, int] = {}
    for timestamp, action in orders:
        if action in (-1, 0, 1):
            by_bar[int(timestamp)] = int(action)

    curve: list[tuple[int, float]] = []
    peak, max_drawdown, in_market, pending = rules.initial_capital, 0.0, 0, None
    for index, bar in enumerate(bars):
        if pending is not None:
            account.apply(pending, bar, index, float(bar["open"]))
            pending = None
        if account.side and account.entry is not None and (rules.fill == "next_open" or account.entry["index"] < index):
            account.exit_levels(bar, index)
        if account.side and account.entry is not None and rules.max_bars is not None \
                and index - account.entry["index"] + (1 if rules.fill == "next_open" else 0) >= rules.max_bars:
            account.close(bar, index, float(bar["close"]), "max_bars")
        action = by_bar.get(int(bar["timestamp"]))
        if action is not None:
            if rules.fill == "close":
                account.apply(action, bar, index, float(bar["close"]))
            else:
                pending = action
        if account.side:
            in_market += 1
        equity = account.equity(float(bar["close"]))
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, 1 - equity / peak if peak > 0 else 0.0)
        curve.append((int(bar["timestamp"]), equity))

    open_trade = None
    if account.side and account.entry is not None and bars:
        last = float(bars[-1]["close"])
        gross = account.side * account.qty * (last - account.entry["price"])
        open_trade = {
            "side": "long" if account.side > 0 else "short",
            "entry_time": account.entry["time"], "entry_price": account.entry["price"],
            "qty": account.qty, "pnl": gross - account.entry["fee"],
            "return": (gross - account.entry["fee"]) / account.entry["notional"],
            "bars": len(bars) - 1 - account.entry["index"],
        }
    return {
        "stats": _stats(bars, curve, account, rules, max_drawdown, in_market),
        "trades": account.trades,
        "open_trade": open_trade,
        "curve": _thin(curve, rules.curve_points),
        "monthly": _monthly(curve, rules.initial_capital),
    }


def _stats(
    bars: list[dict], curve: list[tuple[int, float]], account: _Account, rules: TradeRules,
    max_drawdown: float, in_market: int,
) -> dict[str, Any]:
    trades = account.trades
    final = curve[-1][1] if curve else rules.initial_capital
    wins = [t for t in trades if t["pnl"] > 0]
    gains = sum(t["pnl"] for t in wins)
    losses = -sum(t["pnl"] for t in trades if t["pnl"] <= 0)
    first_close = float(bars[0]["close"]) if bars else 0.0
    span = curve[-1][0] - curve[0][0] if len(curve) > 1 else 0
    years = span / _SECONDS_PER_YEAR
    reasons: dict[str, int] = {}
    for t in trades:
        reasons[t["exit_reason"]] = reasons.get(t["exit_reason"], 0) + 1
    return {
        "bars": len(bars),
        "start": curve[0][0] if curve else None,
        "end": curve[-1][0] if curve else None,
        "initial_capital": rules.initial_capital,
        "final_equity": final,
        "net_return": final / rules.initial_capital - 1,
        "annual_return": (final / rules.initial_capital) ** (1 / years) - 1 if years >= 1 / 12 and final > 0 else None,
        "buy_and_hold": float(bars[-1]["close"]) / first_close - 1 if bars and first_close else None,
        "trades": len(trades),
        "win_rate": len(wins) / len(trades) if trades else None,
        "profit_factor": (gains / losses if losses else None) if trades else None,
        "gross_profit": gains,
        "gross_loss": losses,
        "avg_trade": statistics.fmean(t["return"] for t in trades) if trades else None,
        "best_trade": max(t["return"] for t in trades) if trades else None,
        "worst_trade": min(t["return"] for t in trades) if trades else None,
        "avg_bars": statistics.fmean(t["bars"] for t in trades) if trades else None,
        "max_drawdown": max_drawdown,
        "exposure": in_market / len(bars) if bars else 0.0,
        "fees": account.fees,
        "sharpe": _sharpe(curve),
        "exit_reasons": reasons,
    }


def _sharpe(curve: list[tuple[int, float]]) -> float | None:
    """Annualized Sharpe ratio of the per-bar returns, with no risk-free rate."""
    if len(curve) < 3:
        return None
    returns = [b / a - 1 for (_, a), (_, b) in itertools.pairwise(curve) if a > 0]
    if len(returns) < 2:
        return None
    sd = statistics.pstdev(returns)
    if not sd:
        return None
    step = statistics.median(b - a for (a, _), (b, _) in itertools.pairwise(curve))
    return statistics.fmean(returns) / sd * math.sqrt(_SECONDS_PER_YEAR / step) if step > 0 else None


def _thin(curve: list[tuple[int, float]], points: int) -> list[list[float]]:
    if len(curve) <= points:
        return [[t, v] for t, v in curve]
    every = math.ceil(len(curve) / points)
    thinned = curve[::every]
    if thinned[-1] != curve[-1]:
        thinned.append(curve[-1])
    return [[t, v] for t, v in thinned]


def _monthly(curve: list[tuple[int, float]], initial: float) -> list[dict[str, Any]]:
    months: list[dict[str, Any]] = []
    start = initial
    for timestamp, equity in curve:
        month = datetime.fromtimestamp(timestamp, tz=UTC).strftime("%Y-%m")
        if not months or months[-1]["month"] != month:
            if months:
                start = months[-1]["_end"]
            months.append({"month": month, "_start": start, "_end": equity})
        else:
            months[-1]["_end"] = equity
    return [{"month": m["month"], "return": m["_end"] / m["_start"] - 1 if m["_start"] else None} for m in months]
