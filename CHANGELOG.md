# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- **Aggregations across markets name roles, not markets.** A script declares
  `INPUTS = ["a", "b"]` and reads a role with `ctx.input("a", start_ts, end_ts)`;
  `COMBINE_SQL` names the roles as columns (each input's `close`, or the column
  `INPUT_FIELDS` gives). A run binds each role to a connection of the project: stored in
  `plugin_library` as `INSTANCE_TYPE` (`{"script", "inputs"}`) under the run's name.
  `AggregationWorker` computes every run into a table of its own
  (`output_table_name(project, run)`), checks the inputs against `table_allowed`, and
  serves `ctx.input()` only for the roles a run binds. New on the worker:
  `describe_run()`, `connection_input()` and `drop_output()`. A script with `INPUTS`
  is a template and never runs by itself; scripts without it run as before.
- `AggregationContext(db, inputs={role: (table, filters)})` gives `ctx.input()` outside
  the worker.
- `tradingkit.collector.connection_storage()` and `ConnectionStorage`: the table a
  connection writes and the filters that select its rows, read from the source's
  declarations without running it. The collector uses it too.
- **`tradingkit.backtest.simulate_trades()` and `TradeRules`.** Trades (timestamp,
  action) orders over bars as an account would: fills at the next bar's open or the
  signal bar's close, slippage and fees, position sizing by percent of equity, a fixed
  amount or the risk to a stop, and stop-loss, take-profit, trailing-stop and time exits,
  a stop counting before a target within one bar and a gap filling at the open. Returns
  the stats (net and annual return, buy and hold, drawdown, Sharpe, win rate, profit
  factor, fees, exit reasons), the trades, the open trade, a thinned equity curve and
  monthly returns. Actions are 1, -1 and 0, so what a strategy's signals mean stays the
  caller's choice. `TradeRules.from_dict()` reads form input.
- **Synthetic markets as a formula.** An aggregation script may declare
  `FORMULA = "zscore(se3 - dk1, 96)"` instead of combine(): its names are its roles, the
  inputs are aligned on timestamp and the formula is computed with Polars, parsed and
  never executed. Arithmetic, abs/sqrt/log/exp, min/max/avg across series and
  mean/std/zscore/change/pct/lag over windows, whose widest sets the warmup.
  `tradingkit.formula.parse()` and `evaluate()`; `describe_cross_source()` reports the
  formula.
- **A connection can keep only recent history.** `keep_days` in its config: the collector
  deletes the market's older rows every `prune_interval_s` (an hour by default) and
  starts history and gap recovery no earlier, so they are not fetched again.
  `ClickHouseManager.delete_unit_rows_before()` deletes one market's rows.
- The sandbox spawner sets a worker's process limit only when the worker runs under a
  uid of its own. RLIMIT_NPROC counts every process and thread of the user, so under a
  shared uid (the tests, or a spawner without `--uid-first`) it counted the host's processes
  and left a worker unable to start a thread; on CI that hung a test. Workers also cap
  tokio's thread pool (`TOKIO_WORKER_THREADS`) like the others.
- The collector gives a source its connection's `start_date` in `self.config`, so a
  source can fetch the history the collector will ask for in parts.
- `Stream` refuses a step that is not a positive whole number, an unknown `updates` and a
  table that is not a string. Streams are read from a script without running it, so
  `step=QUARTER` used to arrive as the string "QUARTER" and fail later in the collector.
- **Runs mend themselves when an input fills a gap.** `AggregationWorker` records, per
  `heal_bucket_s` (an hour by default), how many rows of each input a run was computed
  from, in the table `aggregation_coverage`. When an input later gains rows inside that
  span, as when a connection backfills a gap, the run's output from the first such bucket
  is deleted and computed again. The record survives restarts; `drop_output()` and
  `drop_project()` clear it. New on `ClickHouseManager`: `count_rows_by_bucket()`,
  `load_coverage()`, `save_coverage()`, `drop_coverage()` and `delete_rows_since()`.

## [0.6.0] - 2026-11-18

Plugins written by other people can run safely: a sandbox executor, plain-value worker
replies and guarded data access. Sources become classes with named streams, and a new
output plugin, `Sink`, delivers results.

### Changed
- **Breaking: sources are classes only, with a new contract.** A source implements
  `history()` for one request and `live()` for one connection; `setup()` and `close()` are
  optional and run once. The framework now owns the HTTP session (`self.session`), retries
  (`max_retries`, `retry_delay_s`), rate limits, the reconnect loop (`reconnect_delay_s`,
  `max_reconnect_delay_s`) and treating a silent connection as dead (`stale_timeout_s`).
  The `historical()`/`realtime()` function style and the `result =` expression style are
  gone, as is overriding `get_historical_data()` and `stream()`.
- **`AggregationWorker` runs only what is in `plugin_library`.** It used to fall back to
  the host app's `BUILTIN_AGGREGATIONS`, so a fresh install ran templates nobody had added
  and polled tables that did not exist. Builtins remain available as templates.
- **Breaking: `AggregationWorker(db, executor=None, table_allowed=None, poll_interval=60)`
  runs cross-source scripts in the executor's workers.** The host no longer executes them:
  it reads their declarations (`describe_cross_source()`) and rows
  (`run_cross_source_combine()`) as plain data and answers `ctx.query()` itself, only for
  tables `table_allowed` accepts. Each project writes to its own table,
  `output_table_name(namespace, OUTPUT_TABLE)`, which always starts with `agg_`.
  `COMBINE_SQL` scripts now get their Materialized Views installed (nothing did before),
  once per version of the code, and MVs whose script is gone or changed are dropped.
  `run_once()` does one pass; `status` holds each script's last error. `_run_one()` and
  `_load_python_scripts()` are gone.
- **Built-in OHLCV bars fold from the market's base timeframe**, the finest one stored,
  instead of assuming `1m`: a source with a `5m` or `1s` stream gets every multiple of it.
  `_ensure_ohlcv_agg()` raises `ValueError` for a timeframe the base cannot build, and
  `fetch_data_df()` reads the base. **Breaking:** `count_closed_bars()` and
  `invalidate_derived()` take the base timeframe; `invalidate_derived()` used to treat
  every timeframe but `1m` as derived, and would have deleted a `5m` source's own rows.
- **Weekly bars start on Mondays everywhere.** Built-in bars started on Mondays while
  aggregation folds, Materialized Views and closed-bar counts started on Thursdays (the
  epoch's weekday), so the same week differed between them. `get_period_start()` follows
  the same rule.
- **`PluginExecutor.open_source()`** returns a `SourceSession`. Under `SubprocessExecutor`
  it is a long-lived worker process per connection: the plugin is set up once, streaming
  is isolated for the first time, and a dead worker is restarted.
- **Sources describe their data as named `Stream`s** (`table`, `columns`, `step`, `updates`,
  `history`) instead of `TABLE_NAME`. Behaviour follows from these properties: `step` enables
  gap recovery, `updates` picks replacing or appending storage. A connection selects one
  stream with `config["stream"]`, defaulting to the first declared.
- `ScriptSource` no longer executes a script to answer questions about it: table name,
  timestamp unit and capabilities are read from the source text.
- `DataCollector` no longer requires a symbol or a start date. A connection without a start
  date collects live data only, with no backfill or gap recovery.
- `DataCollector` reports `reconnecting` with the reason, and runs gap recovery as soon as
  a stream reconnects rather than at the next interval.
- **Live runs see what a backtest sees.** `Pipeline.run_live()` used to call strategies on
  every update of the forming bar and started with no history. Bars now close by the clock
  (end plus `bar_close_delay_s`) from the exchange's own record, with `provisional=True` on
  signals when that record can't be had, and indicators are warmed up first.
- `DataCollector` closes bars the same way, storing the official bar and calling
  `on_bar_closed`.
- **The license is declared as the SPDX expression `BUSL-1.1`**, which setuptools 77+
  requires; the old table form stops building in 2027.

### Added
- **`bucket_sql(column, seconds)`** is `get_period_start()` as a ClickHouse expression, so
  SQL and Python agree on where a bar starts.
- **C++ indicators run in a worker (`tradingkit.cpp`).** `preview_cpp_indicator()`
  compiles a file exporting `extern "C" void indicator_compute(...)` with the system g++
  and loads it from memory, so worker temp directories can stay `noexec`. Includes are
  limited to `ALLOWED_INCLUDES`, compile errors raise `ScriptError` with the line, and
  `cpp_params()` reads parameter defaults from calls like
  `read_int(params_json, "period", 20)`.
- **Script errors name their line.** Strategy and indicator scripts raise `ScriptError`
  with the line that failed, for example `Line 2: NameError: ...`, and its `line`.
- **`SandboxExecutor` and the sandbox spawner (`python -m tradingkit.executor._spawner`).**
  Every plugin runs in a worker started inside another container, each worker as its
  own Unix user with memory, CPU-time, process and file-size limits, and every process
  it leaves behind killed and its files deleted before the user is reused. Compute work
  and sources/sinks go to separate spawners, so only the latter need a network.
- **`executor.call("module:function", kwargs, on_request=...)`** runs a trusted function
  in a worker; the function asks the host for data through its `host` argument instead
  of holding database credentials. `preview_indicator_script()` and
  `describe_aggregation_spec()` are built for it.
- **`AggregationScript.fold()` builds bars of any timeframe at query time.** It reads the
  script's `aggregation(unit)` folds against its `SOURCE_TABLE` and folds the market's
  deduplicated base rows in ClickHouse (`fold_buckets()`, `Fold.ch_expr()`), so a custom
  aggregation works for custom timeframes without per-bucket tables. `base_timeframe()`
  finds the finest stored timeframe to fold from.
- **`_execute(strict=True)` raises `ClickHouseQueryError`** with the server's message
  instead of logging it and returning `[]`.
- **`Sink` / `ScriptSink`**, the output plugin. `send()` makes one delivery; a long-lived
  connection opens in `setup()`. The framework retries and restores the connection, and
  `PluginExecutor.open_sink()` runs it in its own worker under `SubprocessExecutor`.
- A `secret` parameter type, for values a UI must not show back.
- `Strategy.on_tick()`, optional, for every update of the forming bar in a live run.
  Signals from it carry `tick=True`. It cannot be backtested.
- `run_strategy_script()` and `PluginExecutor.run_strategy_script()`: one way to run a
  vectorized strategy over closed bars, used for both history and live. Sandboxed under
  `SubprocessExecutor`.
- `param_problems()`: reports unknown keys, types and scopes in `__params__`.
- `TransientSourceError`, for failures a plugin wants retried.
- **`script_params()` / `resolve_params()`** in `tradingkit.core.script_ast`: one exec-free
  reader for `__params__`, with values cast to the declared type and checked against
  `options`, `min` and `max`. A host app can build a form from a script and keep the values
  outside its code.

### Security
- **Worker output is no longer unpickled.** A plugin runs in the same process that writes
  the worker's reply, so it could forge a pickle that executed code in the host. Replies
  now carry built-in values only and are decoded by `_wire.plain_loads()`, which never
  imports or constructs a class.
- **Fold arguments cannot read other data:** subqueries, joins, dictionaries and table
  functions are rejected, and folding queries run with `readonly=2`.
- **Sources cannot write to the host's tables.** A stream names its own table, so a
  source could insert rows into `plugin_library` or any other table. `DataCollector`
  takes `table_allowed(name)` and refuses every other table.

### Fixed
- **`tradingkit.__version__` said `0.1.0`.** It now reads the installed package version.
- **A failed insert reopens the native ClickHouse connection.** After ClickHouse
  restarted, the connection was left holding an unread result and refused every insert
  until the app was restarted.
- **Query parameters accept lists and tuples**, rendered as escaped SQL tuples for `IN`.
- **Minute bars were stored under the connection's timeframe.** A source always writes
  one-minute units, but `_ConnectionWorker` labelled them with `conn["timeframe"]`, so a
  `1h` connection filled the `1h` label with 60s-spaced rows that charts drew as hourly
  candles. Rows are now always labelled `1m`, a different requested timeframe logs a
  warning, and higher timeframes come from aggregation as designed.
- **`fetch_data()` could return one timestamp several times.** The live stream re-inserts
  the forming bar, and without `FINAL` every unmerged row reached the caller.
- **Gap recovery never filled a gap inside stored data.** `_batch_complete()` treated any
  range between the oldest and newest row as complete, so a detected gap was skipped
  without fetching. It now counts the rows actually stored.
- **Gap detection mixed timeframes.** Aggregated bars share the `candles` table and hid
  parts of a gap; `get_unit_range()` and `find_unit_gaps()` now filter on the timeframe.
- **Bars and indicators stayed stale after a recovered gap.** Filling a gap now drops the
  derived data from that point, and aggregation counts the closed bars exactly instead of
  estimating. Aggregation also reads `FINAL`, so a forming bar's volume is not summed twice.
- **Custom timeframes were treated as one minute.** `DependencyResolver` looked timeframes
  up in a fixed table with a 60s fallback, so `7m` computed warmup and bucketing as `1m`.
  It now uses `parse_timeframe()`.
- **Built-in indicators computed nothing.** `fetch_ohlcv_aggregated()` filtered on
  `timeframe = '60'` while rows are labelled `1m`, so the resolver always found no data.
- **Indicators covered the oldest bars only.** The resolver fetched with the default limit
  of 10000 in ascending order, so a longer history was cut off before the visible range.
- **TA-Lib indicators could not run under `SubprocessExecutor`.** The class was defined
  inside a function, so it could not be pickled.
- **`__params__` defaults were silently lost on indicators.** `_extract_defaults()` matched
  with a regex that stopped at the first `}`, so the documented nested declaration parsed
  to nothing. It now goes through `resolve_params()`.

## [0.5.0] - 2026-08-09

Analytics come back as user-written plugins, and the regression 0.4.0 shipped is fixed.

### Fixed
- **Regression from 0.4.0: a strategy written in the style 0.4.0 documents crashed the
  resolver.** `Signal` lost its mandatory fields in 0.4.0, but `DependencyResolver` still
  read `sig_dict["signal_type"]` and `sig_dict["confidence"]` by direct indexing, so a
  `process()` returning `{"action": "short", "price": 74.5}` — literally the documented
  free-record form — raised `KeyError`. Worse, the surrounding `try/except` only wrapped
  `plugin.process()`, not the store, so the error took the whole call down instead of
  skipping one bar; anything driving the resolver (in this repo, `/api/strategies/compute`)
  died with it. The 0.4.0 migration covered the `on_bar` path and missed the older
  `process()` one entirely.
- `store_signal()` now takes the whole record. `strategy_signals` still has fixed columns,
  so recognised fields fill them (absent ones default rather than being invented) and
  **everything else is kept in the metadata JSON**, which `fetch_signals()` merges back on
  read. That round-trip is covered by a test on purpose: parking a field somewhere the
  reader doesn't know about is exactly how `price` used to disappear. Storing signals under
  the strategy's own schema — the way `DataCollector` already stores arbitrary source
  schemas — is the cleaner end state and a separate migration; this keeps the data intact
  until then. The old keyword form still works.
- **`ScriptStrategy` never injected `Signal` into the script namespace**, only into
  `on_bar()`'s per-call one. A `process()`-style script — the form the resolver drives —
  therefore raised `NameError: name 'Signal' is not defined` on the example the docs give.
  Same injection `ScriptAggregation` already does for `SourceRef`.

### Added
- **`Metric` / `MetricContext` / `ScriptMetric`** — analytics over a finished run: a scalar
  (win rate), a table for charting (equity curve), or a trade list. `compute(ctx)` runs over
  the whole window, unlike an Indicator's value-per-bar, and `ctx.data` / `ctx.signals` are
  the same `IndicatorContext` everything else already uses, so analytics over signals is the
  same primitive as indicators over prices.
- Declared like indicators — `Pipeline(metrics={"pnl": ...})` → `PipelineResult.metrics` —
  or run ad hoc with `result.compute(metric)` on either result type. A metric that fails
  (asking for a column these signals don't have, say) reports `None` and logs why rather
  than discarding the run.
- `load_metric_plugin()` and an empty `BUILTIN_METRICS`, mirroring strategies and
  aggregations: **the framework still ships no metrics**. That is the point rather than an
  omission — computing PnL means knowing which column value means "open a long", which is
  the strategy author's vocabulary, not the framework's. The host app registers its own via
  `TRADINGKIT_METRICS_MODULE`, and this repo's live under `plugins/metrics/` with every
  vocabulary word (`action_col`, `price_col`, `open_long`, `open_short`, `close`) as a
  parameter, so the same metric works against a schema its author never saw.

  This closes the gap 0.4.0 shipped with. Verified against the case that motivated all of
  it: the short sequence that the old built-in pairing reported as a fabricated long trade
  of +5 now comes back as two short trades totalling +25, and a position still open at the
  end is reported instead of vanishing.

## [0.4.0] - 2026-07-30

Signals stop being a rigid struct, and two silent-corruption bugs in cross-source
aggregation are fixed.

**This release deliberately ships without backtest metrics.** The built-in trade pairing
was removed because it reported wrong numbers convincingly (details below), and its
replacement — user-written `Metric`/`ScriptMetric` analytics, where PnL is computed from
columns you name rather than from a guess about what `"buy"` means — is the next piece of
work, not part of this release. A backtest currently returns the data, the indicator
series and `signals_df`; anything derived is yours to compute until then. Nothing is
better than confidently wrong.

### Fixed — cross-source aggregation on a shared table
- **`SourceRef` had no way to say *which rows* of a table it means**, so cross-source
  aggregation was unusable on the schema a real collector produces. `ensure_cross_source_mv()`
  generated its Materialized View with no `WHERE` at all, which is fine when each source has
  its own table (`candles_btc_usdt`, as every docstring assumed) and silently wrong when one
  shared `candles` table is keyed by exchange/symbol/timeframe — which is what the default
  source script writes. `argMaxState(field, timestamp) GROUP BY timestamp` then picks
  whichever series won at each timestamp, so **one column interleaved BTC, ETH and SOL
  prices**: found by dogfooding a BTC−ETH spread that came back all zeros because both
  aliases were reading the same rows, with individual timestamps carrying an ETH price in
  the "btc" column. No error, no warning, a perfectly plausible series of garbage.
  `SourceRef(..., filters={"exchange": "whitebit", "symbol": "BTC_USDT"})` now pins it, and
  the same filters go into `backfill_cross_source()` — a mismatch there would leave clean
  live data sitting on mixed history. Filters are equality-only by design: a MV body is DDL
  and cannot be parameterized, so a free-form `WHERE` string would have to be defended by a
  hand-written SQL validator, the approach already bypassed once in this codebase
  (`"btc -- eth"` initially passed as two valid operators). Instead the column name goes
  through the existing identifier grammar and the value through the same `_fmt()` escaping
  every ordinary query value uses.
- **`setup_aggregation()` now refuses to build an ambiguous aggregation** instead of
  producing one that mixes series. If a source table holds more than one distinct
  `exchange`/`symbol`/`timeframe` and the filters don't pin it, it raises and names the
  column. A table that genuinely holds one series still needs no filters, so the
  one-table-per-source style is unaffected. This is the check whose absence let the bug
  above survive to production data rather than failing at creation.
- **`AggregationContext` filtered source data by the *project name*.**
  `AggregationWorker._run_one()` did `exchange = script_info.get("exchange", namespace)` and
  `fetch_from_unit_table()` applied `exchange = %(ex)s` unconditionally, so an aggregation in
  a project called `my_bot`, over data tagged `whitebit`, matched zero rows — `combine()`
  returned `[]`, the worker hit `if not rows: return`, and nothing was logged. The script had
  no legal way out either: `ctx.query()` took no exchange and `AggregationContext` was never
  injected into the script namespace. The root cause was one variable serving two roles —
  tagging the aggregation's *output* rows and filtering its *input* data. They are now
  separate: the namespace still tags output (so existing rows stay readable by
  `_get_last_ts`), while input comes from the aggregation's own
  `SOURCE_FILTERS = {"exchange": "whitebit"}`, with `ctx.query(..., exchange=...)` for
  per-query narrowing. `fetch_from_unit_table`'s `exchange` argument is optional now, since
  a mandatory filter with nothing sensible to put in it is what caused this.

### Removed
- **Built-in trade pairing and PnL are gone**: `PipelineResult.trades`,
  `PipelineResult.summary()`, `Trade`, `BacktestRunner._pair_signals()`, and every
  trade-derived property of `BacktestResult` (`total_trades`, `winning_trades`,
  `losing_trades`, `win_rate`, `total_pnl`, `avg_pnl`, `max_drawdown`, `summary()`).
  They did not merely lack short support — they **reported numbers that were wrong**, and
  plausibly so. Verified against the real code: a short strategy going `sell@100 → buy@90`
  then `sell@95 → buy@80` (a genuine +25) came back as *one* trade, `side="buy"`,
  `entry@90 → exit@95`, `pnl=+5`, `win_rate=100%` — it silently re-paired the signals as a
  long, invented a trade the strategy never took, and dropped both real shorts. An
  unclosed position vanished with no trace, and a signal without a `price` made `.trades`
  raise `AttributeError` outright, which is every non-OHLCV source. The root cause was
  structural: pairing had to *guess* intent from `Signal.type == "buy"`, because the
  framework has no notion of a position and a strategy therefore had no way to say what it
  meant. Interpreting a strategy's own vocabulary belongs to analytics that are told which
  columns mean what — arriving as `Metric`/`ScriptMetric` in a following release.
- `Signal.is_buy` / `Signal.is_sell` — they existed only to feed that pairing.
- `Strategy.parameters` — assigned in `__init__` and read by nothing in the framework.

### Changed
- **Breaking: `Signal` has no mandatory fields.** It was the one entity in the framework
  with a rigid schema, while `IndicatorContext` — "wraps *any* Polars DataFrame" — requires
  only a `timestamp` column and `DataSource` rows are free-form. `Signal` is now a sparse
  timestamped record whose schema the strategy author owns: `Signal(action="short",
  price=42.0, zscore=4.2)`, or a plain `dict` from `on_bar()` with no import at all.
  `type`, `confidence`, `metadata` and `price` are no longer declared fields; nothing is
  privileged. The framework *adds* exactly two fields rather than requiring any:
  `timestamp`, and `gap_recovered` on the live path.
- `PipelineResult.signals_df` / `BacktestResult.signals_df` — the emitted signals as a
  `pl.DataFrame`, columns unioned across signals so bars that carried different fields
  simply leave nulls. Since `timestamp` is always present, `IndicatorContext(signals_df)`
  works directly: analytics over signals is the same primitive as indicators over prices.
- `run()` no longer copies `row["close"]` onto `signal.price`. That assumed the source was
  OHLCV and privileged one field name; a strategy that wants a price has `bar.close` and
  puts it there itself.

### Fixed
- `Signal.to_dict()` silently dropped fields. `signal.price = x` wrote to `__dict__` while
  `to_dict()` serialised only the declared fields plus `metadata`, so **every signal the
  API returned had lost its price**. With one field store this cannot recur by
  construction rather than by vigilance — and `from_dict(to_dict(sig)) == sig` now holds
  for anything, including fields the framework added.
- A `Strategy` subclass whose own `__init__` skipped `super().__init__()` had no `.config`
  at all, so `get_required_indicators()` raised `AttributeError` on it — and writing such
  an `__init__` is the normal case, since a strategy's parameters are usually plain
  arguments. `config` now falls back to an empty dict per instance (verified not shared
  between instances, which a mutable class attribute would have been).

## [0.3.0] - 2026-07-28

### Fixed
- Indicators were computed **positionally**, with no awareness of time: TA-Lib and numpy
  see an array index, not a timestamp, so a missing bar silently made two rows minutes
  apart look adjacent, corrupting every value after the gap. `Pipeline.run()` and
  `run_live()` now close gaps with **real re-fetched bars** before computing anything.
  Synthetic fill was ruled out empirically, not assumed: verified against real TA-Lib, a
  single NaN poisons every subsequent output for the rest of the array — RSI, EMA, and
  even SMA, despite being a plain sliding window — so NaN-reindexing would have been
  strictly worse than doing nothing, and forward-filling invents prices that never traded.
  Backfilled bars run fully through the strategy and can produce signals of their own,
  marked `Signal.gap_recovered=True` so a consumer can treat a signal for an
  already-minutes-old bar differently from a live one. A bar re-fetched to close a gap
  that then arrives on the stream anyway (delayed rather than lost) is deduplicated
  instead of processed twice.
- `run_live()` had no error handling around indicator or strategy execution — one bad bar
  killed the entire live stream. It now logs and continues, matching `run()`.
- `Pipeline` had no rate limiting at all, so gap backfill would have hammered an
  exchange's REST API unthrottled. Throttling moved onto `DataSource` (see below), which
  both `Pipeline` and `DataCollector` now share.
- `DataCollector` executed user-submitted connection scripts **in-process, bypassing the
  executor layer entirely** — unsandboxed, while the same code went through
  `SubprocessExecutor` under `Pipeline`. The historical/gap path now fetches via the
  executor. Streaming still does not: `PluginExecutor` has no streaming method, which is
  a separate design problem, tracked rather than hidden.

### Changed
- **Breaking: `Pipeline.run()` / `run_live()` take `timeframe: int`**, not `str | int`.
  Callers convert human strings themselves with the same public `parse_timeframe()` the
  framework used internally.
- **Breaking: `timeframe` is now an integer *step* in the source's own timestamp unit**,
  not "seconds". `DataSource.timestamp_unit` (`"s"`/`"ms"`/`"us"`/`"ns"`, default `"s"`)
  declares that unit; the framework consults it only where wall-clock time meets stored
  timestamps, since gap and batching arithmetic was already unit-relative. This unblocks
  sub-second sources (ticks, order books), which were previously blocked by naming
  convention rather than by any actual arithmetic. `parse_timeframe(tf, unit="s")` gained
  an optional unit (`parse_timeframe("4h", "ms") == 14_400_000`). Known cosmetic gap left
  alone deliberately: aggregate tables are still named `{raw_table}_{bucket}s`, so a
  millisecond source produces a name like `..._60000s`; renaming would touch existing
  ClickHouse tables and the name takes part in no computation.
- **`ConnectionScriptSource` merged into `ScriptSource`**, which now subclasses
  `DataSource` (the old name remains as an alias). The two classes did the same thing —
  `exec()` user code — and differed only in calling convention, which meant
  `DataCollector` could reuse nothing from `DataSource`. One class now handles three
  conventions: a script that subclasses `DataSource` (full control — it can override
  `detect_gaps`, `batch_gaps`, `fetch_gap`, `rate_limit`, `timestamp_unit`), module-level
  `historical()`/`realtime()` functions (existing connection scripts, unchanged), or
  `result = ...` (UI editor).
- Script sources compile and exec **once** instead of on every call — each gap fetch used
  to recompile the whole script. The cache is keyed on the code itself and excluded from
  pickling, since `SubprocessExecutor` pickles the source per call and a live namespace
  can hold sockets or HTTP sessions.
- `DataSource` gained `detect_gaps()`, `batch_gaps()`, `fetch_gap()` and `rate_limit()`,
  all with working defaults. Gap batching and rate limiting previously lived on
  `_ConnectionWorker`, though both are properties of the exchange rather than of the
  worker; they now have one home, shared by every consumer.

## [0.2.0] - 2026-07-27

### Added
- `Aggregation` / `ScriptAggregation` — declarative cross-source aggregation (e.g. a
  BTC/ETH spread) via a gap-closing two-Materialized-View pattern: each declared source
  writes its own partial state into a shared `AggregatingMergeTree` table, keyed by
  timestamp, so arrival order never matters and a late or backfilled source still
  completes the row correctly once it lands. Two ways to compute the result: `combine_sql()`
  (a validated SQL expression over the declared sources, kept live by ClickHouse on every
  insert — no polling) or `combine(ctx, start_ts, end_ts)` (a Python fallback for logic SQL
  can't express, with the same signature and `ctx.query()` power as the free-function
  `aggregate()` convention it replaces). `ScriptAggregation` / `load_aggregation_plugin()`
  mirror `ScriptStrategy` / `load_strategy_plugin` so a SaaS UI editor can create new
  aggregations without a framework release. The within-table bucket-aggregation mechanism
  (`Fold`, `AggregationScript.is_ch_mv()`) is unrelated and unchanged.
- Gap-recovery backoff: `_gap_loop()` now backs off exponentially (capped at
  `max_gap_interval_s`, default 3600s) after consecutive no-progress cycles instead of
  retrying at a fixed interval forever, and surfaces a distinct `"stalled"` connection
  status once `stalled_threshold` (default 5) consecutive failures is crossed — previously
  a connection stuck offline for hours was indistinguishable from one with a small, normal
  gap except by watching the gap count grow, and retried at the same rate regardless.

### Fixed
- `LiveFeed` routed live candles by `(symbol, timeframe)` only, dropping `exchange` — two
  different exchanges streaming the same symbol/timeframe would cross-deliver each other's
  candles to the wrong subscribers. `publish()` now requires `exchange`; the routing key
  also normalizes exchange case, since the same exchange was observed tagged inconsistently
  (`"whitebit"` vs `"WhiteBit"`) depending on connection config.
- `insert_unit_batch()` never wrote a `timeframe` column, so every row landed with
  ClickHouse's empty-string default regardless of the connection's actual timeframe,
  breaking any timeframe-filtered query. `ensure_raw_table()` now also migrates existing
  tables that predate this column.
- Cross-source aggregation hardcoded `UInt64` for the timestamp type baked into its
  `AggregateFunction(argMax, ..., UInt64)` state; a real `DataSource`'s schema (e.g.
  `Int64`, as produced by `ensure_raw_table`'s own Polars-to-ClickHouse type mapping)
  made ClickHouse reject the write outright (`CANNOT_CONVERT_TYPE`) rather than silently
  coercing it. Found via dogfooding the aggregation demo against a real data source, not
  anticipated up front — now covered by a regression test using an `Int64` source table.

## [0.1.1] - 2026-07-25

### Fixed
- `Signal.is_buy` / `Signal.is_sell` were referenced by `PipelineResult.trades` but never
  defined on `Signal`, so calling `.trades` on any pipeline result raised `AttributeError`.
- `Fold` SQL generation for parametric ClickHouse aggregate functions: quantile-family
  functions (`quantile`, `topK`, `groupArrayMovingAvg`, ...) now repeat their parameter in
  the `Merge` expression as ClickHouse requires — previously `quantileMerge` silently
  computed the median instead of the requested level. Combinator-style functions (`sumIf`,
  ...) keep their arguments flat and correctly drop them in `Merge`.
- Stale `mholovion` GitHub username references replaced with `nivolon` throughout the
  source and docs.

### Added
- `[project.urls]` (Homepage/Repository/Issues) in `pyproject.toml`.
- Compiled C++ kernels are now cached by content hash (SHA-256) inside the runner process,
  so identical `.so` bytes are written to disk and `dlopen`'d only once instead of on every
  call.

### Changed
- **License changed from MIT to [Business Source License 1.1](LICENSE)**, converting
  automatically to Apache License 2.0 on 2030-07-25 (or sooner, per version — see the
  license text). Free for effectively all use, including internal commercial use; the
  one thing it excludes is offering `tradingkit` as a competing hosted/managed
  backtesting or charting service. This is **not retroactive** — v0.1.0 remains
  available under MIT under its original terms; only v0.1.1 and later are BUSL-1.1.
- CI (`ruff check .`) was silently exposed to drift: an unbounded `ruff>=0.4` dev dependency
  meant a newer ruff release could expand its default rule set and fail CI on old code with
  no corresponding change. Pinned to `ruff>=0.4,<0.17` and cleaned up ~180 pre-existing
  findings (mostly `UP037`/`UP045`/`I001` mechanical modernizations, plus a handful of real
  fixes — `ClassVar` on shared class-level lookup tables, `Self` return types on `__aenter__`,
  a `NaN` check rewritten from `x == x` to `math.isnan(x)`, and a Docker container-stop
  callback rebound through `functools.partial` instead of a loop-variable-capturing lambda);
  `ruff check .` is now clean.

## [0.1.0] - 2026-07-21

Initial public release.
