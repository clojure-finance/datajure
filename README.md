# Datajure v2

[![Clojars Project](https://img.shields.io/clojars/v/com.github.clojure-finance/datajure.svg)](https://clojars.org/com.github.clojure-finance/datajure)
[![CI](https://github.com/clojure-finance/datajure/actions/workflows/tests.yml/badge.svg)](https://github.com/clojure-finance/datajure/actions/workflows/tests.yml)
[![cljdoc](https://cljdoc.org/badge/com.github.clojure-finance/datajure)](https://cljdoc.org/d/com.github.clojure-finance/datajure/CURRENT)

**One function. Six keywords. Two expression modes.**

Datajure is a Clojure data manipulation library built on [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset). It provides a clean, composable query DSL for filtering, transforming, grouping, and aggregating tabular data.

```clojure
(require '[datajure.core :as core])

;; Filter, group, aggregate — one call
(core/dt ds
  :where #dt/e (> :year 2008)
  :by [:species]
  :agg {:n core/N :avg #dt/e (mn :mass)})

;; Window functions — same keywords, no new concepts
(core/dt ds
  :by [:species]
  :within-order [(core/desc :mass)]
  :set {:rank #dt/e (win/rank :mass)})

;; Thread for multi-step pipelines
(-> ds
    (core/dt :set {:bmi #dt/e (/ :mass (sq :height))})
    (core/dt :by [:species] :agg {:avg-bmi #dt/e (mn :bmi)})
    (core/dt :order-by [(core/desc :avg-bmi)]))
```

Datajure is a **syntax layer**, not an engine — it compiles `#dt/e` expressions to vectorized operations and delegates all computation to `tech.v3.dataset`. Every result is a standard `tech.v3.dataset` dataset. Full interop with tablecloth, Clerk, Clay, and the Scicloj ecosystem.

## Installation

Add to your `deps.edn`:

```clojure
{:deps {com.github.clojure-finance/datajure {:mvn/version "2.0.4"}}}
```

Datajure requires Clojure 1.12+ and Java 21+.


## The Key Insight: `:by` × `:set`/`:agg`

Two orthogonal keywords produce four distinct operations with no new concepts:

|            | No `:by`            | With `:by`          |
|------------|---------------------|---------------------|
| **`:set`** | Column derivation (+ whole-dataset window if `win/*` present) | **Partitioned window** |
| **`:agg`** | Whole-table summary | Group aggregation   |

```clojure
;; Column derivation — add/update columns, keep all rows
(core/dt ds :set {:bmi #dt/e (/ :mass (sq :height))})

;; Group aggregation — collapse rows per group
(core/dt ds :by [:species] :agg {:n core/N :avg-mass #dt/e (mn :mass)})

;; Whole-table summary — collapse everything
(core/dt ds :agg {:total #dt/e (sm :mass) :n core/N})

;; first-val / last-val — OHLC construction (pre-sort, then aggregate)
(-> trades
    (core/dt :order-by [(core/asc :time)])
    (core/dt :by [:sym :date]
             :agg {:open  #dt/e (first-val :price)
                   :close #dt/e (last-val :price)
                   :hi    #dt/e (mx :price)
                   :vol   #dt/e (sm :size)}))

;; wavg / wsum — VWAP and weighted sum
(core/dt trades :by [:sym :date]
         :agg {:vwap #dt/e (wavg :size :price)
               :vol  #dt/e (wsum :size :price)})

;; Partitioned window — compute within groups, keep all rows
(core/dt ds
  :by [:species]
  :within-order [(core/desc :mass)]
  :set {:rank #dt/e (win/rank :mass)
        :cumul #dt/e (win/cumsum :mass)})

;; Whole-dataset window — no :by, entire dataset is one partition
(core/dt ds
  :within-order [(core/asc :date)]
  :set {:cumret #dt/e (win/cumsum :ret)
        :prev   #dt/e (win/lag :price 1)})
```

## Expression Mode: `#dt/e`

`#dt/e` is a reader tag that rewrites bare keywords to column accessors. It returns an AST object that `dt` interprets — vectorized, nil-safe, and pre-validated.

```clojure
;; With #dt/e — terse, nil-safe, vectorized
(core/dt ds :where #dt/e (> :mass 4000))
(core/dt ds :set {:bmi #dt/e (/ :mass (sq :height))})

;; Without — plain Clojure functions (always works)
(core/dt ds :where #(> (:mass %) 4000))
(core/dt ds :set {:bmi #(/ (:mass %) (Math/pow (:height %) 2))})
```

`#dt/e` is opt-in. Users who prefer plain Clojure functions can ignore it entirely.

### Nil-safe by default

```clojure
;; #dt/e comparisons with nil → false (no NullPointerException)
(core/dt ds :where #dt/e (> :mass 4000))

;; Arithmetic with nil propagates nil
(core/dt ds :set {:x #dt/e (+ :a :b)})  ;; nil + 1 → nil

;; Replace nil with a default
(core/dt ds :set {:mass #dt/e (coalesce :mass 0)})

;; Nil-safe division — zero or nil denominator → nil
(core/dt ds :set {:ratio #dt/e (div0 :numerator :denominator)})
```

### Special forms

```clojure
;; Multi-branch conditional
(core/dt ds :set {:size #dt/e (cond
                                (> :mass 5000) "large"
                                (> :mass 3500) "medium"
                                :else "small")})

;; Local bindings
(core/dt ds :set {:adj #dt/e (let [bmi (/ :mass (sq :height))
                                   base (if (> :year 2010) 1.1 1.0)]
                               (* base bmi))})

;; Boolean composition, membership, range
(core/dt ds :where #dt/e (and (> :mass 4000) (not (= :species "Adelie"))))
(core/dt ds :where #dt/e (in :species #{"Gentoo" "Chinstrap"}))
(core/dt ds :where #dt/e (between? :year 2007 2009))
```

### Reusable expressions

`#dt/e` returns first-class AST values. Store in vars, reuse across queries:

```clojure
(def bmi #dt/e (/ :mass (sq :height)))
(def high-mass #dt/e (> :mass 4000))

(core/dt ds :set {:bmi bmi})
(core/dt ds :where high-mass)
(core/dt ds :by [:species] :agg {:avg-bmi #dt/e (mn bmi)})
```

## `:select` — Polymorphic Column Selection

```clojure
(core/dt ds :select [:species :mass])                    ;; explicit list
(core/dt ds :select :type/numerical)                     ;; all numeric columns
(core/dt ds :select :!type/numerical)                    ;; all non-numeric
(core/dt ds :select #"body-.*")                          ;; regex match
(core/dt ds :select [:not :id :timestamp])               ;; exclusion
(core/dt ds :select {:species :sp :mass :m})             ;; select + rename
(core/dt ds :select (core/between :month-01 :month-12))  ;; positional range (inclusive)
```

## Window Functions

Available via `win/*` inside `#dt/e`. Work in `:set` context — with `:by` for partitioned windows, or without `:by` for whole-dataset windows:

```clojure
;; Partitioned window — grouped by permno
(core/dt ds
  :by [:permno]
  :within-order [(core/asc :date)]
  :set {:rank    #dt/e (win/rank :ret)
        :lag-1   #dt/e (win/lag :ret 1)
        :cumret  #dt/e (win/cumsum :ret)
        :regime  #dt/e (win/rleid :sign-ret)})

;; Whole-dataset window — no :by, entire dataset is one partition
(core/dt ds
  :within-order [(core/asc :date)]
  :set {:cumret #dt/e (win/cumsum :ret)
        :prev   #dt/e (win/lag :price 1)})
```

Functions: `win/rank`, `win/dense-rank`, `win/row-number`, `win/lag`, `win/lead`, `win/cumsum`, `win/cummin`, `win/cummax`, `win/cummean`, `win/rleid`, `win/delta`, `win/ratio`, `win/differ`, `win/mavg`, `win/msum`, `win/mdev`, `win/mmin`, `win/mmax`, `win/ema`, `win/fills`, `win/scan`.

### Adjacent-Element Ops

Inspired by q's `deltas` and `ratios` — eliminate verbose lag patterns:

```clojure
(core/dt ds :by [:permno] :within-order [(core/asc :date)]
    :set {:ret       #dt/e (- (win/ratio :price) 1)    ;; simple return
          :price-chg #dt/e (win/delta :price)           ;; first differences
          :changed   #dt/e (win/differ :signal)})       ;; boolean change flag
```

### Rolling Windows & EMA

```clojure
(core/dt ds :by [:permno] :within-order [(core/asc :date)]
    :set {:ma-20   #dt/e (win/mavg :price 20)      ;; 20-day moving average
          :vol-20  #dt/e (win/mdev :ret 20)         ;; 20-day moving std dev
          :hi-52w  #dt/e (win/mmax :price 252)      ;; 52-week high
          :ema-10  #dt/e (win/ema :price 10)})      ;; 10-day EMA
```

### Forward-Fill

```clojure
(core/dt ds :by [:permno] :within-order [(core/asc :date)]
    :set {:price #dt/e (win/fills :price)})         ;; carry forward last known
```

### Cumulative Scan

Generalized cumulative operation inspired by APL/q's scan (`\`). Supports `+`, `*`, `max`, `min` — the killer use case is the wealth index:

```clojure
(core/dt ds :by [:permno] :within-order [(core/asc :date)]
    :set {:wealth  #dt/e (win/scan * (+ 1 :ret))    ;; cumulative compounding
          :cum-vol #dt/e (win/scan + :volume)         ;; = win/cumsum
          :runmax  #dt/e (win/scan max :price)})      ;; running maximum
```

## Row-wise Functions

Cross-column operations within a single row via `row/*`:

```clojure
(core/dt ds :set {:total  #dt/e (row/sum :q1 :q2 :q3 :q4)
                  :avg-q  #dt/e (row/mean :q1 :q2 :q3 :q4)
                  :n-miss #dt/e (row/count-nil :q1 :q2 :q3 :q4)})
```

Functions: `row/sum` (nil as 0), `row/mean`, `row/min`, `row/max` (skip nil), `row/count-nil`, `row/any-nil?`.


## Statistical Transforms

Column-level statistical transforms via `stat/*` inside `#dt/e`. All functions are nil-safe: nil values are excluded from reference statistics (mean, sd, percentiles) and produce nil outputs.

```clojure
;; Standardize: (x - mean) / sd — returns all-nil if sd is zero
(core/dt ds :set {:z #dt/e (stat/standardize :ret)})

;; Demean: x - mean(x)
(core/dt ds :set {:dm #dt/e (stat/demean :ret)})

;; Winsorize at 1% tails — clips to [p, 1-p] percentile bounds
(core/dt ds :set {:wr #dt/e (stat/winsorize :ret 0.01)})

;; Compose with arithmetic
(core/dt ds :set {:scaled #dt/e (* 2 (stat/demean :x))})

;; Cross-sectional standardization per group
(core/dt ds :by [:date] :set {:z #dt/e (stat/standardize :signal)})
```

Functions: `stat/standardize`, `stat/demean`, `stat/winsorize`.

## Joins

Standalone function with cardinality validation and merge diagnostics. Supports regular joins (`:inner`, `:left`, `:right`, `:outer`) and as-of joins (`:asof`).

```clojure
(require '[datajure.join :refer [join]])

(join X Y :on :id :how :left)
(join X Y :on [:firm :date] :how :inner :validate :m:1)
(join X Y :left-on :id :right-on :key :how :left :report true)
;; [datajure] join report: 150 matched, 3 left-only, 0 right-only

;; Thread with dt
(-> (join X Y :on :id :how :left :validate :m:1)
    (core/dt :where #dt/e (> :year 2008)
             :agg {:total #dt/e (sm :revenue)}))
```

## As-of Joins

Inspired by q's `aj`. For each left row, find the last right row where `right-key <= left-key` within an exact-match group. All left rows are always preserved; unmatched rows get nil for right columns.

The **last column** in `:on` (or `:left-on`/`:right-on`) is the asof column — preceding columns are exact-match keys.

```clojure
(require '[datajure.join :refer [join]])

;; Trade-quote matching: each trade gets the last prevailing bid/ask
;; sym is exact-match, time is asof (last quote where quote-time <= trade-time)
(join trades quotes :on [:sym :time] :how :asof)

;; Asymmetric key names
(join trades quotes
      :left-on  [:sym :trade-time]
      :right-on [:sym :quote-time]
      :how :asof)

;; Single asof key — no exact grouping, match across entire dataset
(join left right :on [:time] :how :asof)

;; With cardinality validation (right side only)
(join trades quotes :on [:sym :time] :how :asof :validate :m:1)

;; Pipeline: match ticks to bars, then compute OHLC
(-> (join bars ticks :on [:sym :time] :how :asof)
    (core/dt :where #dt/e (> :px 100)
             :order-by [(core/asc :time)]))
```

**Result schema:** all left columns in original order, plus right non-key columns appended. Conflicting non-key column names are suffixed `:right.<name>` (same convention as regular joins).

**`:validate` for `:asof`:** only the right side is checked (`:1:1` and `:m:1` require unique right keys). The left side is never checked since all left rows always appear.

## Reshaping

```clojure
(require '[datajure.reshape :refer [melt]])

(-> ds
    (melt {:id [:species :year] :measure [:mass :flipper :bill]})
    (core/dt :by [:species :variable] :agg {:avg #dt/e (mn :value)}))
```

## Utilities

```clojure
(require '[datajure.util :as du])

(du/describe ds)                                ;; summary stats → dataset
(du/describe ds [:mass :height])                ;; subset of columns
(du/clean-column-names messy-ds)                ;; "Some Ugly Name!" → :some-ugly-name
(du/mark-duplicates ds [:id :date])             ;; adds :duplicate? column
(du/drop-constant-columns ds)                   ;; remove zero-variance
(du/coerce-columns ds {:year :int64 :mass :float64})
```

## File I/O

```clojure
(require '[datajure.io :as dio])

(def ds (dio/read "data.csv"))
(def ds (dio/read "data.parquet"))    ;; needs tech.v3.libs.parquet
(def ds (dio/read "data.tsv.gz"))     ;; gzip auto-detected
(dio/write ds "output.csv")
```

Supported: CSV, TSV, Parquet, Arrow, Excel, Nippy. Gzipped variants auto-detected.

## Bucketing with `xbar`

Floor-division bucketing inspired by q's `xbar`. Primary use case is computed `:by` for time-series bar generation:

```clojure
;; Numeric bucketing in :by — price buckets of width 10
(core/dt ds :by [(core/xbar :price 10)] :agg {:n core/N :avg #dt/e (mn :volume)})

;; 5-minute OHLCV bars
(-> trades
    (core/dt :order-by [(core/asc :time)])
    (core/dt :by [(core/xbar :time 5 :minutes) :sym]
             :agg {:open  #dt/e (first-val :price)
                   :close #dt/e (last-val :price)
                   :vol   #dt/e (sm :size)
                   :n     core/N}))

;; Also usable inside #dt/e as a column derivation
(core/dt ds :set {:bucket #dt/e (xbar :price 5)})
```

Supported temporal units: `:seconds`, `:minutes`, `:hours`, `:days`, `:weeks`. Returns nil for nil input.

## Quantile Binning with `cut`

Equal-count (quantile) binning inside `#dt/e`. The optional `:from` mask computes breakpoints from a **reference subpopulation** and applies them to **all rows** — the reference and binned populations can be different sizes. This directly models the NYSE-breakpoints pattern used in empirical finance:

```clojure
;; Basic: 5 equal-count bins across all rows
(core/dt ds :set {:size-q #dt/e (cut :mktcap 5)})

;; NYSE breakpoints: compute quintile breakpoints from NYSE stocks only,
;; apply to all stocks (NYSE + AMEX + NASDAQ)
(core/dt ds :set {:size-q #dt/e (cut :mktcap 5 :from (= :exchcd 1))})

;; :from accepts any #dt/e boolean expression
(core/dt ds :set {:size-q #dt/e (cut :mktcap 5 :from (and (= :exchcd 1) (> :year 2000)))})

;; Pre-computed boolean flag column also works
(core/dt ds :set {:size-q #dt/e (cut :mktcap 5 :from :nyse?)})

;; Per-date NYSE breakpoints — the canonical CRSP usage
(-> crsp
    (core/dt :where #dt/e (= (month :date) 6))
    (core/dt :by [:date]
             :set {:size-q #dt/e (cut :mktcap 5 :from (= :exchcd 1))}))
```

## Rename

```clojure
(core/rename ds {:mass :weight-kg :species :penguin-species})
```

## Concise Namespace

Short aliases for power users:

```clojure
(require '[datajure.concise :refer [mn sm md sd ct nuniq fst lst wa ws N dt asc desc rename pass-nil]])

(dt ds :by [:species] :agg {:n N :avg #(mn (:mass %))})
```

| Symbol | Full name |
|--------|-----------|
| `mn` | mean |
| `sm` | sum |
| `md` | median |
| `sd` | standard deviation |
| `mx` | max (column maximum) |
| `mi` | min (column minimum) |
| `ct` | element count |
| `nuniq` | count-distinct |
| `fst` | first-val (first element) |
| `lst` | last-val (last element) |
| `wa` | wavg (weighted average) |
| `ws` | wsum (weighted sum) |
| `standardize` | stat/stat-standardize |
| `demean` | stat/stat-demean |
| `winsorize` | stat/stat-winsorize |
| `between` | positional range selector |

## Notebook Integration

### Clay (Scicloj ecosystem)

```clojure
(require '[datajure.clay :as dc])
(dc/install!)   ;; auto-renders datasets, #dt/e exprs, describe output

;; Or explicit wrapping:
(dc/view ds)
(dc/view-expr #dt/e (/ :mass (sq :height)))
(dc/view-describe (du/describe ds))
```

Start a Clay notebook:
```clojure
(require '[scicloj.clay.v2.api :as clay])
(clay/make! {:source-path "notebooks/datajure_clay_demo.clj"})
```

### Clerk

```clojure
(require '[datajure.clerk :as dc])
(dc/install!)   ;; registers custom Clerk viewers
```

## REPL

`*dt*` holds the last dataset result (like `*1`), bound by nREPL middleware:

```clojure
user=> (core/dt ds :by [:species] :agg {:n core/N})
;; => dataset...

user=> (core/dt core/*dt* :order-by [(core/desc :n)])
```

Enable in `.nrepl.edn`: `{:middleware [datajure.nrepl/wrap-dt]}`

## Error Messages

Structured `ex-info` with suggestions:

```clojure
(core/dt ds :set {:bmi #dt/e (/ :mass :hieght)})
;; => ExceptionInfo: Unknown column :hieght in :set expression
;;    Did you mean: :height (edit distance: 1)
;;    Available columns: :species :year :mass :height :flipper

(core/dt ds :set {:a #dt/e (/ :x 1)} :agg {:n core/N})
;; => ExceptionInfo: Cannot combine :set and :agg. Use -> threading.
```

## Evaluation Order

Fixed, regardless of keyword order in the call:

```
1. :where  — filter rows
2. :set or :agg  — derive or aggregate (mutually exclusive)
3. :select — keep listed columns
4. :order-by — sort final output
```

## Architecture

```
User writes:   #dt/e (/ :mass (sq :height))
                          ↓
               AST (pure data, serializable)
                          ↓
               compile-expr → fn [ds] → column vector
                          ↓
               tech.v3.datatype.functional (dfn)
                          ↓
               tech.v3.dataset (columnar, JVM, fast)
```

Datajure is a syntax layer. `#dt/e` expressions compile to an AST, which `compile-expr` translates to vectorized `dfn` operations on `tech.v3.dataset` column vectors. Computation is entirely delegated to the underlying engine; the DSL itself adds only the parsing and dispatch overhead.

## Namespace Guide

| Namespace | Purpose |
|-----------|---------|
| `datajure.core` | `dt`, `N`, `mean`, `sum`, `median`, `stddev`, `variance`, `max*`, `min*`, `count*`, `asc`, `desc`, `pass-nil`, `rename`, `xbar`, `cut`, `between`, `*dt*` |
| `datajure.expr` | AST nodes, compiler, `#dt/e` reader tag |
| `datajure.concise` | Short aliases for power users |
| `datajure.window` | Window function implementations |
| `datajure.row` | Row-wise function implementations |
| `datajure.stat` | Statistical transforms: `stat/standardize`, `stat/demean`, `stat/winsorize` |
| `datajure.util` | `describe`, `clean-column-names`, `duplicate-rows`, etc. |
| `datajure.io` | Unified `read`/`write` dispatching on file extension |
| `datajure.reshape` | `melt` for wide→long |
| `datajure.join` | `join` with `:validate`, `:report`, and `:how :asof` |
| `datajure.asof` | As-of join engine: `asof-search`, `asof-indices`, `asof-match`, `build-result` |
| `datajure.nrepl` | nREPL middleware for `*dt*` auto-binding |
| `datajure.clerk` | Rich Clerk notebook viewers |
| `datajure.clay` | Clay/Kindly notebook integration |

## Design Principles

1. **`dt` is a function** — not a macro. Debuggable, composable, predictable.
2. **`:where` always filters** — conditional updates go inside `:set`.
3. **Keyword lifting requires `#dt/e`** — no implicit magic.
4. **Nil-safe by default** — comparisons → false, arithmetic → nil, `coalesce` replaces.
5. **Expressions are values** — store in vars, compose, reuse.
6. **One function, not 28** — one `dt`, six keywords, two expression modes.
7. **Threading for pipelines** — don't reinvent `->`, use it.
8. **Errors are data** — structured `ex-info`, Levenshtein suggestions, extensible.
9. **Syntax layer, not engine** — delegate to tech.v3.dataset. Full interop.

## Development

Tests run automatically on every push to `main` via GitHub Actions. CI runs the core test suites (core, concise, util, io, reshape, join, asof, stat) via `bin/run-tests.sh`. The nrepl, clerk, and clay test suites require optional deps and are run locally only. When adding a new core test namespace, add it to `bin/run-tests.sh` to include it in CI.

```bash
# Start nREPL
clj -A:nrepl

# Run core tests (same as CI)
bash bin/run-tests.sh

# Run all tests locally (including optional-dep suites)
clj -A:nrepl -e "
  (load-file \"test/datajure/core_test.clj\")
  (load-file \"test/datajure/concise_test.clj\")
  (load-file \"test/datajure/util_test.clj\")
  (load-file \"test/datajure/io_test.clj\")
  (load-file \"test/datajure/reshape_test.clj\")
  (load-file \"test/datajure/join_test.clj\")
  (load-file \"test/datajure/asof_test.clj\")
  (load-file \"test/datajure/nrepl_test.clj\")
  (load-file \"test/datajure/clerk_test.clj\")
  (load-file \"test/datajure/clay_test.clj\")
  (load-file \"test/datajure/stat_test.clj\")
  (clojure.test/run-tests
    'datajure.core-test 'datajure.concise-test 'datajure.util-test
    'datajure.io-test 'datajure.reshape-test 'datajure.join-test
    'datajure.asof-test 'datajure.nrepl-test 'datajure.clerk-test
    'datajure.clay-test 'datajure.stat-test)"
```

246 tests, 798 assertions.

## Prior Work

Datajure v1 was a routing layer across three backends (tablecloth, clojask, geni/Spark). v2 takes a different approach: a single, opinionated syntax layer directly on tech.v3.dataset, with a DSL design inspired by R's data.table and Julia's DataFramesMeta.jl.

- v1 repo: https://github.com/clojure-finance/datajure/tree/v1

Special thanks to [YANG Ming-Tian](https://github.com/skylee03) for the original v1 implementation.

## License

Copyright © 2024–2025 Centre for Investment Management, HKU Business School.

Distributed under the Eclipse Public License version 2.0.
