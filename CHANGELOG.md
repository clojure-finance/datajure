# Changelog

All notable changes to Datajure will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.0.9] - 2026-04-20

A post-alpha audit pass reconciling the library with data.table-style semantics, plus a handful of correctness fixes uncovered by REPL verification of the DSL's per-partition execution paths.

### Changed

- **`qtile` now uses per-partition breakpoints when combined with exact keys in `:by`.** Previously `:by [:date (qtile :mktcap 5)]` computed breakpoints once from the whole dataset (and silently produced wrong answers for per-date cross-sectional sorts — the canonical CRSP / Fama-French size sort). It now partitions by the exact keys first, then resolves `qtile` against each sub-dataset, matching data.table / dplyr / q:
  ```clojure
  ;; Per-date size quintiles — now works the obvious way
  (core/dt stocks :by [:date (core/qtile :mktcap 5)]
           :agg {:mean-ret #dt/e (mn :ret)})

  ;; Per-date NYSE-style breakpoints applied to all stocks (Fama-French size sort)
  (core/dt stocks :by [:date (core/qtile :mktcap 5 :from #dt/e (= :exchcd 1))]
           :agg {:mean-ret #dt/e (mn :ret)})
  ```
  An audit of every other DSL feature (`stat/*`, aggregations inside composite `#dt/e`, `cut :from`, `win/*`, `row/*`, `xbar`, `join :asof`/`:window`) confirmed `qtile` was the only outlier — every other feature already ran per-partition by virtue of living inside `apply-group-*`. Pure-`qtile` `:by` (no exact keys) still resolves globally since there is nothing to partition by.

### Added

- **`cast` — long→wide reshaping.** Complement to `melt`. For each unique combination of `:id` column values, pivots the `:from` column's distinct values into new columns filled from the `:value` column. New column names derived from `:from` values (keywords pass through; strings converted via `keyword`). Supports `:agg` for duplicate cells and `:fill` for missing cells (default nil). `melt` / `cast` round-trip correctly.
  ```clojure
  ;; Reverse a melt
  (-> ds
      (melt {:id [:species :year] :measure [:mass :flipper]})
      (cast {:id [:species :year] :from :variable :value :value}))

  ;; With aggregation for duplicate (id, from) cells
  (cast ds {:id [:date :sym] :from :metric :value :val :agg dfn/mean})
  ```

- **`cast` accepts a single-keyword `:id`.** Normalised to a one-element vector, matching `melt`. Previously a single-keyword `:id` errored with `"Don't know how to create ISeq from: clojure.lang.Keyword"`.

### Fixed

- **`row/sum`, `row/mean`, `row/min`, `row/max` on all-nil rows.** The four row-wise aggregators declared `:float64` readers but claimed in their docstrings to return `nil` when every input is nil. Primitive float readers cannot hold `nil` — the value was silently coerced to `NaN`, contradicting the docstring. Readers are now `:object`, so all-nil rows honestly return `nil`. `row-min` / `row-max` also cast non-nil results to `double` to preserve the always-numeric-result convention.

- **`wavg` / `wsum` with mismatched column lengths.** Previously silently truncated (when the weight column was shorter) or NPE'd (when the value column was shorter). Now throws a structured `:unequal-column-lengths` `ex-info` with `:dt/op`, `:dt/weight-length`, and `:dt/value-length` in `ex-data`.

- **`join :asof :tolerance` on datetime asof keys.** Previously produced a raw `java.lang.ClassCastException` from an unguarded `(double dt-value)` coercion inside `within-tolerance?`. Now validates numerically-compatible asof key types upfront and throws a structured `:join-tolerance-non-numeric` `ex-info` with actionable guidance (convert to epoch-milliseconds). Symmetric and asymmetric join key shapes (`:on` vs `:left-on`/`:right-on`) both report the correct column names in `ex-data`.

- **`describe` on all-missing numeric columns.** When every value in a numeric column was missing, `dfn/standard-deviation` returned `-0.0` while `mean`, `min`, `max`, `median`, and percentiles correctly returned `nil`, producing an incoherent summary row. `describe-column` now routes all-missing numeric columns through the nil-filled branch (same as non-numeric columns).

- **`parse-window-spec` now strict-validates window spec shape.** Previously the implementation destructured `[a b c :as wspec]` and silently dropped any trailing elements. Malformed specs now throw a structured `:join-invalid-window` `ex-info` — trailing junk, wrong arity, non-numeric endpoints, non-vector specs, and misplaced unit keywords are all rejected upfront. Valid `[lo hi]`, `[lo hi unit]`, and `[lo unit hi]` shapes behave exactly as before.

- **`count-distinct` now excludes `nil`.** The fn included `nil` in its distinct count, contradicting its docstring ("non-nil values"). Fixed by filtering `some?` before `distinct`.

- **`qtile` and `cut` now use the same breakpoint algorithm.** `qtile`'s `percentile-breakpoints` previously used a floor-index approximation that produced different breakpoints than `cut-bucket` (which uses `dfn/percentiles`). The two now share `dfn/percentiles`, so `qtile :mktcap 5` and `#dt/e (cut :mktcap 5)` produce identical bins for the same population.

- **Breakpoint-at-exact-value semantics unified across `qtile` and `cut`.** `bin-via-breakpoints` now uses `<=` (values equal to a breakpoint go to the lower bin), matching `cut-bucket`'s `java.util.Arrays/binarySearch` exact-match behaviour. The previously passing `qtile-from-basic` test's assertion was wrong; corrected to reflect actual semantics.

- **`xbar` / `xbar-bucket` now use `Math/floorDiv`.** Previously used `quot`, which truncates toward zero — so negative values bucketed incorrectly relative to q's `xbar` semantics. E.g. `(xbar -3 5)` now returns `-5` rather than `0`.

- **`validate-expr-cols` and `validate-select-cols` NPE on zero-column datasets.** Both helpers computed `(->> avail-names (map ...) (sort-by second) first)`, which returned `nil` when `avail-names` was empty. `(second nil)` yielded `nil`, and `(<= nil 3)` threw `NullPointerException`. Guarded with `(and closest ...)` in the suggestion-emission branch.

### Developer experience

- **`win/scan` op normalisation mirrors `win/each-prior`.** The parser now prefers `sym->op` for the canonical keyword, so invalid scan ops like `/` resolve to `:div` (consistent with the rest of the codebase) rather than a keyword literally spelled with a slash. Valid scan ops (`+`, `*`, `max`, `min`) are unchanged in both AST and runtime.

### Internal

- **Damerau-Levenshtein deduplication.** The edit-distance implementation used by typo suggestions was duplicated byte-for-byte in `datajure.core` and `datajure.expr`. Extracted to the public `datajure.expr/damerau-levenshtein` as the single source of truth; both `validate-expr-cols` / `validate-select-cols` and `suggest-op` now call it.

- **Dead `win-ops` set removed** from `datajure.expr` (unused, and stale — missing `win/scan` and `win/each-prior`).

### Testing

- Test count: 310 → 318 (+8 new deftests, +89 assertions). CI subset: 268/901 → 276/989. All passing.
- New deftests: `row-fns-all-nil-returns-nil-not-nan`, `wavg-wsum-unequal-lengths`, `asof-tolerance-non-numeric-error-test`, `describe-all-missing-numeric`, `wjoin-invalid-window-shape-test`, `qtile-per-group-breakpoints`, `qtile-from-with-exact-key`. The existing `qtile-combined-with-keyword` test was strengthened from a column-names-only check to an assertion of per-group bin counts (would fail loudly against the old global-breakpoint implementation).

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

[Unreleased]: https://github.com/clojure-finance/datajure/compare/v2.0.9...HEAD
[2.0.9]: https://github.com/clojure-finance/datajure/compare/v2.0.8...v2.0.9
[2.0.8]: https://github.com/clojure-finance/datajure/compare/v2.0.7...v2.0.8
[2.0.7]: https://github.com/clojure-finance/datajure/compare/v2.0.6...v2.0.7
[2.0.6]: https://github.com/clojure-finance/datajure/compare/v2.0.5...v2.0.6
