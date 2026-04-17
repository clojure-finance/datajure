# Changelog

All notable changes to Datajure will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/clojure-finance/datajure/compare/v2.0.6...HEAD
[2.0.6]: https://github.com/clojure-finance/datajure/compare/v2.0.5...v2.0.6
