# Changelog

All notable changes to Datajure will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.8] - 2026-04-18

### Added

- **`qtile :from` — reference-subpopulation breakpoints.** `qtile` now accepts an optional `:from` keyword argument: a `#dt/e` boolean expression or a boolean column keyword that selects a reference subpopulation for computing breakpoints. Breakpoints are computed from the filtered subset and applied to all rows — the same semantics as `#dt/e (cut :col n :from pred)`. Classic use case is NYSE-style breakpoints: `(qtile :mktcap 5 :from #dt/e (= :exchcd 1))` computes quintile boundaries from NYSE stocks and applies them to all stocks.
  ```clojure
  (core/dt stocks :by [(core/qtile :mktcap 5 :from #dt/e (= :exchcd 1))]
           :agg {:n core/nrow :mean-ret #dt/e (mn :ret)})
  ```

- **`win/each-prior` — generalized adjacent-element operator.** Applies any binary operator to `f(x[i], x[i-1])` for each element. First element → nil; nil propagates if either value is nil. Supports `+`, `-`, `*`, `/`, `max`, `min`, `>`, `<`, `>=`, `<=`, `=`. Generalizes `win/delta` (op=`-`) and `win/ratio` (op=`/`) without their double-casting or zero-guard:
  ```clojure
  (core/dt ds :by [:permno] :within-order [(core/asc :date)]
           :set {:pw-hi #dt/e (win/each-prior max :price)  ;; pairwise max with previous
                 :up?   #dt/e (win/each-prior > :price)})  ;; did price increase?
  ```

- **Bounded as-of joins — `:direction` and `:tolerance`.** `join` with `:how :asof` now accepts two new options:
  - `:direction` — `:backward` (default, last right ≤ left), `:forward` (first right ≥ left), or `:nearest` (closest by abs distance; ties prefer `:backward`).
  - `:tolerance` — numeric max abs distance; matches beyond it produce nil. Requires a numeric asof key.
  ```clojure
  ;; Forward: each trade matched to next available quote
  (join trades quotes :on [:sym :time] :how :asof :direction :forward)

  ;; Nearest: closest quote in either direction
  (join trades quotes :on [:sym :time] :how :asof :direction :nearest)

  ;; Tolerance: reject stale quotes more than 5 seconds old
  (join trades quotes :on [:sym :time] :how :asof :tolerance 5)
  ```
  `asof-search` gains a 4-arity directional variant; `asof-match` gains a 6-arity variant (4-arity delegates with `:backward`/`nil` defaults — fully backward-compatible).

- **Window join (`:how :window`, q's `wj`).** For each left row, aggregates **all** right rows whose asof-key falls within a window around the left row's asof-key. Window spec formats: `[lo hi]`, `[lo hi unit]`, or `[lo unit hi]` where unit is `:seconds`, `:minutes`, `:hours`, `:days`, or `:weeks`. Aggregation map `:agg` accepts `#dt/e` expressions (nil for empty windows) or plain fns (receive a 0-row sub-dataset for empty windows, so `nrow` → 0). All left rows preserved.
  ```clojure
  ;; 5-minute VWAP window
  (join trades quotes
    :on [:sym :time]
    :how :window
    :window [-5 0 :minutes]
    :agg {:avg-bid #dt/e (mn :bid)
          :avg-ask #dt/e (mn :ask)
          :n       core/nrow})
  ```
  `asof.clj` gains a public `window-indices` utility function for programmatic use.

### Testing

- Test count: 273 → 299 (+26 new deftests, +113 assertions). CI subset: 200 → 257 (stat_test.clj added; dataset dep bumped to 8.007 in run-tests.sh). All passing.

## [2.0.7] - 2026-04-17

### Added

- **`qtile` quantile grouping for `:by`.** `core/qtile` is the `:by`-friendly companion to `#dt/e (cut ...)` -- produces an equal-count bin assignment from a column's distribution, computed once from the dataset before grouping. Use it when you want to *group by* quantile rather than *derive a column of* quantile bins:
  ```clojure
  ;; Quintile buckets of market cap
  (core/dt stocks :by [(core/qtile :mktcap 5)]
           :agg {:n core/nrow :mean-ret #dt/e (mn :ret)})
  ;; Result column is auto-named :mktcap-q5

  ;; Per-date size quintiles combined with an exact key
  (core/dt stocks :by [:date (core/qtile :mktcap 5)]
           :agg {:mean-ret #dt/e (mn :ret)})
  ```
  Inspired by R's `cut` and Stata's `xtile`; named to evoke quintile/decile. Companion to `xbar` (equal-width bins) with a symmetric API. Result column name defaults to `<col>-q<n>`, overridable via `:datajure/col` metadata (the same extension point `xbar` uses). `nil` inputs form their own group (nil key). The `:from` option for reference-subpopulation breakpoints was added post-release — see `[Unreleased]`.

### Changed

- **`by->group-fn` now receives the dataset.** Internal refactor: the private `by->group-fn` helper takes the dataset as a parameter so that `:by` markers requiring population-level statistics (like `qtile`) can precompute their breakpoints before grouping. Both call sites (`apply-group-agg`, `apply-group-set`) have been updated. No user-visible behaviour change.

### Testing

- Test count: 267 -> 273 (+6 new deftests, +17 assertions). CI subset: 200 -> 206. All passing.

## [2.0.6] - 2026-04-17

### Added

- **`:within-order` with `:agg`.** `:within-order` can now be combined with `:agg` (with or without `:by`), sorting rows within each partition (or across the whole dataset) before aggregation. This enables order-sensitive aggregations like OHLC bar construction in a single `dt` call:
  ```clojure
  (core/dt trades
    :by [:sym]
    :within-order [(core/asc :time)]
    :agg {:open  #dt/e (first-val :price)
          :close #dt/e (last-val :price)
          :vol   #dt/e (sm :size)
          :n     core/N})
  ```
  Previously this required two `dt` calls (pre-sort via `:order-by`, then aggregate). The restriction that `:within-order` required `:set` has been relaxed to "`:within-order` requires `:set` or `:agg`."
- **`nrow` alias for `N`.** `core/nrow` is now exported alongside `core/N` as a more discoverable full-name alternative for row counting in `:agg`. Both are equivalent.

### Changed

- **`clean-column-names` is now Unicode-aware.** The regex that strips non-identifier characters no longer removes CJK, Cyrillic, Greek, accented Latin, and other non-ASCII letters and digits. Mixed-script column names like `"Some Name (HKD millions)!"` combined with CJK or accented-Latin characters are now preserved intact; only punctuation, whitespace, and symbols are replaced. Pure-ASCII column names behave exactly as before. See `clean-column-names-unicode` in the test suite for coverage of CJK, accented Latin, and mixed-script cases.

### Fixed

- **Edit-distance algorithm for typo suggestions.** The Levenshtein implementation in both `core.clj` (column-name suggestions) and `expr.clj` (op-name suggestions) was producing incorrect edit distances (e.g., `"kitten"` to `"sitting"` returned 5 instead of 3), causing many legitimate suggestions to be dropped. The algorithm has been replaced with a correct Damerau-Levenshtein implementation that also treats single adjacent transpositions as distance 1. Typos like `:hieght` now correctly suggest `:height`.
- **`win/ratio` no longer propagates `Infinity` on zero denominators.** When the previous row's value is zero, `win/ratio` now returns `nil` rather than `Infinity`. This matches the `div0` philosophy and gives the correct result for the canonical simple-return idiom `(- (win/ratio :price) 1)` -- a zero-price observation now yields `nil` for the next row's return, signalling "exclude" rather than contaminating downstream calculations.

### Developer experience

- **`:agg` plain-function footgun detection.** Plain functions passed to `:agg` receive the group dataset, so `#(:mass %)` returns a column vector rather than a scalar -- a common mistake for users coming from `:set` context. Previously this silently produced nonsensical output (a column inside each result cell); now it throws a structured error with guidance pointing to either `#(dfn/mean (:mass %))` or the preferred `#dt/e (mn :mass)`.
- **Unknown ops in `#dt/e` produce structured errors with suggestions.** A typo like `#dt/e (sqrt :x)` previously caused a raw `ClassCastException` at runtime. It now throws an `ex-info` at read time with the suggestion: ``Unknown op `sqrt` in #dt/e expression. Did you mean: `sq`?``. Namespaced ops (`win/*`, `row/*`, `stat/*`) get namespace-aware suggestions: `win/mvag` -> `win/mavg`, `stat/standardise` -> `stat/standardize`.

### Testing

- Test count: 193 -> 200 (+7 new deftests, +72 assertions). All passing.

## [2.0.5] and earlier

Earlier versions are not documented in this changelog. Release history is tracked in the [GitHub releases](https://github.com/clojure-finance/datajure/releases) page and in `PROJECT_SUMMARY.md`'s phase-completion table.

[Unreleased]: https://github.com/clojure-finance/datajure/compare/v2.0.8...HEAD
[2.0.8]: https://github.com/clojure-finance/datajure/compare/v2.0.7...v2.0.8
[2.0.7]: https://github.com/clojure-finance/datajure/compare/v2.0.6...v2.0.7
[2.0.6]: https://github.com/clojure-finance/datajure/compare/v2.0.5...v2.0.6
