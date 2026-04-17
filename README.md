# Datajure v2

[![Clojars Project](https://img.shields.io/clojars/v/com.github.clojure-finance/datajure.svg)](https://clojars.org/com.github.clojure-finance/datajure)
[![CI](https://github.com/clojure-finance/datajure/actions/workflows/tests.yml/badge.svg)](https://github.com/clojure-finance/datajure/actions/workflows/tests.yml)
[![cljdoc](https://cljdoc.org/badge/com.github.clojure-finance/datajure)](https://cljdoc.org/d/com.github.clojure-finance/datajure/CURRENT)

**One function. Seven keywords. Two expression modes.**

Datajure is a Clojure data manipulation library built on [tech.ml.dataset](https://github.com/techascent/tech.ml.dataset). It provides a clean, composable query DSL for filtering, transforming, grouping, and aggregating tabular data.

```clojure
(require '[datajure.core :refer [dt nrow asc desc]])

;; Filter, group, aggregate — one call
(dt ds
  :where #dt/e (> :year 2008)
  :by [:species]
  :agg {:n nrow :avg #dt/e (mn :mass)})

;; Window functions — same keywords, no new concepts
(dt ds
  :by [:species]
  :within-order [(desc :mass)]
  :set {:rank #dt/e (win/rank :mass)})

;; OHLC bars in one call — :within-order with :agg sorts each group first
(dt trades
  :by [:sym]
  :within-order [(asc :time)]
  :agg {:open  #dt/e (first-val :price)
        :close #dt/e (last-val :price)
        :hi    #dt/e (mx :price)
        :lo    #dt/e (mi :price)
        :vol   #dt/e (sm :size)})

;; Thread for multi-step pipelines
(-> ds
    (dt :set {:bmi #dt/e (/ :mass (sq :height))})
    (dt :by [:species] :agg {:avg-bmi #dt/e (mn :bmi)})
    (dt :order-by [(desc :avg-bmi)]))
```

Datajure is a **syntax layer**, not an engine — it compiles `#dt/e` expressions to vectorized operations and delegates all computation to `tech.v3.dataset`. Every result is a standard `tech.v3.dataset` dataset. Full interop with tablecloth, Clerk, Clay, and the Scicloj ecosystem.

## Why Datajure

Datajure takes inspiration from whichever library or language got a given idea right — R's `data.table` (terse query form, single-expression semantics), APL/q/kdb+ (first-class primitives for time-series operations you use every day), Polars (expressions as values, composable vocabulary), Julia's `DataFramesMeta.jl` (one function with keyword arguments, not twenty-eight verbs). The goal is not to be any of them. It is to combine the parts that were genuinely revelations.

Concretely, if you've used:

- **R's `data.table`** — you'll find `DT[i, j, by]` maps directly onto `(dt ds :where i :set-or-agg j :by by)`. Nil handling is cleaner than data.table's `NA`. There is no in-place mutation (Datajure is immutable) and no secondary indexes (`setkey`); tech.v3.dataset's columnar layout is fast enough without them.
- **Python's pandas/Polars** — you get expression objects as values (like Polars' `Expr`), nil-safe comparisons and arithmetic by default, and a single query form rather than a pipeline of a dozen verbs.
- **R's `dplyr` or tidyverse** — you'll find the same pipe-friendly composition (`->` is Clojure's pipe), with less verbosity and without the function-per-verb proliferation.
- **Julia's `DataFramesMeta.jl`** — the `#dt/e` reader tag serves the same role as DFM's `@transform`/`@subset`, but because Clojure has a real reader tag mechanism (rather than macros pretending to parse expressions), it integrates more cleanly with the rest of the language.
- **q/kdb+** — the `win/*` namespace gives you first-class `deltas`, `ratios`, `mavg`, `msum`, `mdev`, `ema`, `fills`, `scan`, plus `wavg`, `wsum`, `first`, `last` as aggregation primitives. `xbar` ships for time-series bar generation. As-of joins are built in.

Datajure's unique wedge is that `#dt/e` expressions are first-class AST values — you can store them in vars and compose them across queries. Build a shared vocabulary once, reuse it everywhere:

```clojure
(def ret     #dt/e (- (win/ratio :price) 1))
(def log-ret #dt/e (log (+ 1 ret)))
(def vol-20d #dt/e (win/mdev ret 20))
(def wealth  #dt/e (win/scan * (+ 1 ret)))

(dt prices :by [:permno] :within-order [(asc :date)]
    :set {:ret ret :log-ret log-ret :vol-20d vol-20d :wealth wealth})
```

No equivalent exists in tablecloth, dplyr, pandas, or data.table.

## Installation

Add to your `deps.edn`:

```clojure
{:deps {com.github.clojure-finance/datajure {:mvn/version "2.0.6"}}}
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
(dt ds :set {:bmi #dt/e (/ :mass (sq :height))})

;; Group aggregation — collapse rows per group
(dt ds :by [:species] :agg {:n nrow :avg-mass #dt/e (mn :mass)})

;; Whole-table summary — collapse everything
(dt ds :agg {:total #dt/e (sm :mass) :n nrow})

;; Partitioned window — compute within groups, keep all rows
(dt ds
  :by [:species]
  :within-order [(desc :mass)]
  :set {:rank #dt/e (win/rank :mass)
        :cumul #dt/e (win/cumsum :mass)})

;; Whole-dataset window — no :by, entire dataset is one partition
(dt ds
  :within-order [(asc :date)]
  :set {:cumret #dt/e (win/cumsum :ret)
        :prev   #dt/e (win/lag :price 1)})
```

`:within-order` also combines with `:agg`, sorting rows within each group before the aggregation runs. This is the one-call OHLC pattern and the reason `first-val` / `last-val` are first-class helpers:

```clojure
(dt trades
    :by [:sym :date]
    :within-order [(asc :time)]
    :agg {:open  #dt/e (first-val :price)
          :close #dt/e (last-val :price)
          :hi    #dt/e (mx :price)
          :vol   #dt/e (sm :size)})

;; VWAP and weighted sum
(dt trades :by [:sym :date]
    :agg {:vwap #dt/e (wavg :size :price)
          :vol  #dt/e (wsum :size :price)})
```

## `dt` Dispatch Modes

`dt` runs a single fixed evaluation order: `:where` → `:set`-or-`:agg` → `:select` → `:order-by`. What the middle step does depends on which other keywords are present:

| `:by`  | `:set`  | `:agg`  | `:within-order` | Mode                                                    |
|--------|---------|---------|-----------------|---------------------------------------------------------|
| —      | plain   | —       | —               | Derive columns over whole dataset                       |
| —      | `win/*` | —       | optional        | Whole-dataset window                                    |
| ✓      | plain   | —       | optional        | Per-group derivation                                    |
| ✓      | `win/*` | —       | optional        | Partitioned window                                      |
| —      | —       | ✓       | optional        | Whole-table aggregate (sorted first if `:within-order`) |
| ✓      | —       | ✓       | optional        | Group aggregate (sorted within group if `:within-order`)|

Disallowed: `:set` and `:agg` in the same call (use `->` threading); `:within-order` without `:set` or `:agg`.

## Expression Mode: `#dt/e`

`#dt/e` is a reader tag that rewrites bare keywords to column accessors. It returns an AST object that `dt` interprets — vectorized, pre-validated, and nil-literal-safe.

```clojure
;; With #dt/e — terse, keyword-lifted, vectorized
(dt ds :where #dt/e (> :mass 4000))
(dt ds :set {:bmi #dt/e (/ :mass (sq :height))})

;; Without — plain Clojure functions (always works)
(dt ds :where #(> (:mass %) 4000))
(dt ds :set {:bmi #(/ (:mass %) (Math/pow (:height %) 2))})
```

`#dt/e` is opt-in. Users who prefer plain Clojure functions can ignore it entirely. See *Expression Mode vs. Plain Functions* below for when to pick which.

### Nil handling

Datajure has a layered nil story rather than blanket "nil-safety". The rules:

| Situation                                             | Behaviour                |
|-------------------------------------------------------|--------------------------|
| Comparison op with a nil *literal* in `#dt/e`         | evaluates to `false`     |
| Arithmetic op with a nil *literal* in `#dt/e`         | returns `nil`            |
| Column-level nils (nil values within a column)        | depends on the `dfn` op  |
| Aggregation helpers (`mn`/`sm`/`md`/`sd`/`nrow`/...)  | skip nil; `nil` if all missing (never `0`/`-Inf`/`NaN`) |
| `win/fills :col`                                      | forward-fill nils        |
| `coalesce :col default`                               | replace nils with fallback |
| `div0 num den`                                        | `nil` if denominator is `nil` or zero |
| `win/ratio :col`                                      | `nil` if previous value is `nil` or zero |
| Plain Clojure functions                               | **not** automatic; wrap with `pass-nil` |

```clojure
(dt ds :where #dt/e (> :mass 4000))                  ;; nil-literal → false
(dt ds :set {:mass #dt/e (coalesce :mass 0)})         ;; nil → 0
(dt ds :set {:pe   #dt/e (div0 :price :earnings)})    ;; zero denom → nil
(dt ds :set {:x (pass-nil #(parse-int (:x-str %)))})  ;; wrap plain fn
```

### Special forms

```clojure
;; Multi-branch conditional
(dt ds :set {:size #dt/e (cond
                           (> :mass 5000) "large"
                           (> :mass 3500) "medium"
                           :else "small")})

;; Local bindings
(dt ds :set {:adj #dt/e (let [bmi (/ :mass (sq :height))
                              base (if (> :year 2010) 1.1 1.0)]
                          (* base bmi))})

;; Boolean composition, membership, range
(dt ds :where #dt/e (and (> :mass 4000) (not (= :species "Adelie"))))
(dt ds :where #dt/e (in :species #{"Gentoo" "Chinstrap"}))
(dt ds :where #dt/e (between? :year 2007 2009))
```

### Reusable expressions

`#dt/e` returns first-class AST values. Store them in vars, reuse across queries, compose them into new expressions:

```clojure
(def bmi       #dt/e (/ :mass (sq :height)))
(def high-mass #dt/e (> :mass 4000))
(def obese     #dt/e (> bmi 30))         ;; composition — bmi appears inside another #dt/e

(dt ds :set {:bmi bmi})
(dt ds :where high-mass)
(dt ds :by [:species] :agg {:avg-bmi #dt/e (mn bmi)})
(dt ds :where obese)
```

The mechanism is simple: `#dt/e` returns an AST map, and `(def ...)` captures that value. When the symbol appears inside another `#dt/e`, Clojure evaluates it to its AST value before the outer reader sees it, and the compiler splices it in. No macros, no magic — just values.

### Expression Mode vs. Plain Functions

|                       | `#dt/e` (column-wise)                  | Plain function (context-dependent)     |
|-----------------------|----------------------------------------|----------------------------------------|
| Operates on           | Whole column vectors via `dfn`         | Row map in `:set`/`:where`; group dataset in `:agg` |
| Column access         | Bare keywords: `:mass`                 | `(:mass %)`                            |
| Performance           | Fast — vectorized                      | Slower — per-row call in `:set`/`:where` |
| Nil handling          | Automatic (for literals and helpers)   | Manual (`pass-nil` or explicit checks) |
| Validation            | Pre-execution column checking; Damerau suggestions | Runtime errors only          |
| Best for              | Arithmetic, comparisons, aggregations  | Complex branching, Java interop, non-vectorizable logic |

Prefer `#dt/e` by default. Fall back to plain functions when the computation doesn't map to vectorized ops.

**Footgun to know about in `:agg`:** plain functions receive the *group dataset*, not a row, so `(:mass %)` returns a column vector rather than a scalar. Datajure detects this and throws a structured error since v2.0.6 — but this is why `#dt/e (mn :mass)` is safer than `#(mean (:mass %))`.

## `:select` — Polymorphic Column Selection

```clojure
(dt ds :select [:species :mass])                    ;; explicit list
(dt ds :select :type/numerical)                     ;; all numeric columns
(dt ds :select :!type/numerical)                    ;; all non-numeric
(dt ds :select #"body-.*")                          ;; regex match
(dt ds :select [:not :id :timestamp])               ;; exclusion
(dt ds :select {:species :sp :mass :m})             ;; select + rename
(dt ds :select (between :month-01 :month-12))       ;; positional range (inclusive)
```

## Window Functions

Available via `win/*` inside `#dt/e`. Work in `:set` context — with `:by` for partitioned windows, or without `:by` for whole-dataset windows:

```clojure
;; Partitioned window — grouped by permno
(dt ds
  :by [:permno]
  :within-order [(asc :date)]
  :set {:rank    #dt/e (win/rank :ret)
        :lag-1   #dt/e (win/lag :ret 1)
        :cumret  #dt/e (win/cumsum :ret)
        :regime  #dt/e (win/rleid :sign-ret)})

;; Whole-dataset window — no :by, entire dataset is one partition
(dt ds
  :within-order [(asc :date)]
  :set {:cumret #dt/e (win/cumsum :ret)
        :prev   #dt/e (win/lag :price 1)})
```

Functions: `win/rank`, `win/dense-rank`, `win/row-number`, `win/lag`, `win/lead`, `win/cumsum`, `win/cummin`, `win/cummax`, `win/cummean`, `win/rleid`, `win/delta`, `win/ratio`, `win/differ`, `win/mavg`, `win/msum`, `win/mdev`, `win/mmin`, `win/mmax`, `win/ema`, `win/fills`, `win/scan`.

### Adjacent-Element Ops

Inspired by q's `deltas` and `ratios` — eliminate verbose lag patterns:

```clojure
(dt ds :by [:permno] :within-order [(asc :date)]
    :set {:ret       #dt/e (- (win/ratio :price) 1)    ;; simple return
          :price-chg #dt/e (win/delta :price)          ;; first differences
          :changed   #dt/e (win/differ :signal)})      ;; boolean change flag
```

`win/ratio` returns `nil` (not `Infinity`) when the previous value is zero or nil — the canonical simple-return idiom `(- (win/ratio :price) 1)` therefore produces `nil` after a zero-price row rather than contaminating downstream calculations.

### Rolling Windows & EMA

```clojure
(dt ds :by [:permno] :within-order [(asc :date)]
    :set {:ma-20   #dt/e (win/mavg :price 20)     ;; 20-day moving average
          :vol-20  #dt/e (win/mdev :ret 20)       ;; 20-day moving std dev
          :hi-52w  #dt/e (win/mmax :price 252)    ;; 52-week high
          :ema-10  #dt/e (win/ema :price 10)})    ;; 10-day EMA
```

### Forward-Fill

```clojure
(dt ds :by [:permno] :within-order [(asc :date)]
    :set {:price #dt/e (win/fills :price)})       ;; carry forward last known
```

### Cumulative Scan

Generalized cumulative operation inspired by APL/q's scan (`\`). Supports `+`, `*`, `max`, `min` — the killer use case is the wealth index:

```clojure
(dt ds :by [:permno] :within-order [(asc :date)]
    :set {:wealth  #dt/e (win/scan * (+ 1 :ret))   ;; cumulative compounding
          :cum-vol #dt/e (win/scan + :volume)       ;; = win/cumsum
          :runmax  #dt/e (win/scan max :price)})    ;; running maximum
```

## Row-wise Functions

Cross-column operations within a single row via `row/*`:

```clojure
(dt ds :set {:total  #dt/e (row/sum :q1 :q2 :q3 :q4)
             :avg-q  #dt/e (row/mean :q1 :q2 :q3 :q4)
             :n-miss #dt/e (row/count-nil :q1 :q2 :q3 :q4)})
```

Functions: `row/sum` (nil as 0), `row/mean`, `row/min`, `row/max` (skip nil), `row/count-nil`, `row/any-nil?`.

## Statistical Transforms

Column-level transforms via `stat/*` inside `#dt/e`. All are nil-safe — nil values are excluded from reference statistics and produce nil outputs.

```clojure
;; Standardize: (x - mean) / sd — returns all-nil if sd is zero
(dt ds :set {:z #dt/e (stat/standardize :ret)})

;; Demean: x - mean(x)
(dt ds :set {:dm #dt/e (stat/demean :ret)})

;; Winsorize at 1% tails — clips to [p, 1-p] percentile bounds
(dt ds :set {:wr #dt/e (stat/winsorize :ret 0.01)})

;; Compose with arithmetic
(dt ds :set {:scaled #dt/e (* 2 (stat/demean :x))})

;; Cross-sectional standardization per group
(dt ds :by [:date] :set {:z #dt/e (stat/standardize :signal)})
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
    (dt :where #dt/e (> :year 2008)
        :agg {:total #dt/e (sm :revenue)}))
```

## As-of Joins

Inspired by q's `aj`. For each left row, find the last right row where `right-key <= left-key` within an exact-match group. All left rows are always preserved; unmatched rows get nil for right columns.

The **last column** in `:on` (or `:left-on`/`:right-on`) is the asof column — preceding columns are exact-match keys.

```clojure
(require '[datajure.join :refer [join]])

;; Trade-quote matching: each trade gets the last prevailing bid/ask.
;; sym is exact-match, time is asof (last quote where quote-time <= trade-time)
(join trades quotes :on [:sym :time] :how :asof)

;; Asymmetric key names
(join trades quotes
      :left-on  [:sym :trade-time]
      :right-on [:sym :quote-time]
      :how :asof)

;; With cardinality validation (right side only)
(join trades quotes :on [:sym :time] :how :asof :validate :m:1)
```

**Result schema:** all left columns in original order, plus right non-key columns appended. Conflicting non-key column names are suffixed `:right.<n>` (same convention as regular joins).

**`:validate` for `:asof`:** only the right side is checked (`:1:1` and `:m:1` require unique right keys). The left side is never checked since all left rows always appear.

## Reshaping

```clojure
(require '[datajure.reshape :refer [melt]])

(-> ds
    (melt {:id [:species :year] :measure [:mass :flipper :bill]})
    (dt :by [:species :variable] :agg {:avg #dt/e (mn :value)}))
```

## Utilities

```clojure
(require '[datajure.util :as du])

(du/describe ds)                                ;; summary stats → dataset
(du/describe ds [:mass :height])                ;; subset of columns
(du/clean-column-names messy-ds)                ;; "Some Ugly Name!" → :some-ugly-name (Unicode-aware)
(du/mark-duplicates ds [:id :date])             ;; adds :duplicate? column
(du/drop-constant-columns ds)                   ;; remove zero-variance
(du/coerce-columns ds {:year :int64 :mass :float64})
```

`clean-column-names` preserves non-ASCII characters (CJK, accented Latin, Cyrillic, Greek) — `"市值 (HKD millions)!"` becomes `:市值-hkd-millions`.

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
(dt ds :by [(xbar :price 10)] :agg {:n nrow :avg #dt/e (mn :volume)})

;; 5-minute OHLCV bars
(dt trades
    :by [(xbar :time 5 :minutes) :sym]
    :within-order [(asc :time)]
    :agg {:open  #dt/e (first-val :price)
          :close #dt/e (last-val :price)
          :vol   #dt/e (sm :size)
          :n     nrow})

;; Also usable inside #dt/e as a column derivation
(dt ds :set {:bucket #dt/e (xbar :price 5)})
```

Supported temporal units: `:seconds`, `:minutes`, `:hours`, `:days`, `:weeks`. Returns nil for nil input.

## Quantile Binning with `cut`

Equal-count (quantile) binning inside `#dt/e`. The optional `:from` mask computes breakpoints from a **reference subpopulation** and applies them to **all rows** — the reference and binned populations can be different sizes. This directly models the NYSE-breakpoints pattern used in empirical finance:

```clojure
;; Basic: 5 equal-count bins across all rows
(dt ds :set {:size-q #dt/e (cut :mktcap 5)})

;; NYSE breakpoints: compute quintile breakpoints from NYSE stocks only,
;; apply to all stocks (NYSE + AMEX + NASDAQ)
(dt ds :set {:size-q #dt/e (cut :mktcap 5 :from (= :exchcd 1))})

;; :from accepts any #dt/e boolean expression
(dt ds :set {:size-q #dt/e (cut :mktcap 5 :from (and (= :exchcd 1) (> :year 2000)))})

;; Per-date NYSE breakpoints — the canonical CRSP usage
(-> crsp
    (dt :where #dt/e (= (month :date) 6))
    (dt :by [:date]
        :set {:size-q #dt/e (cut :mktcap 5 :from (= :exchcd 1))}))
```

## Computed `:by` — Custom Grouping Functions

`:by` accepts a plain function of the row in addition to column keywords. Functions can attach `:datajure/col` metadata to control the result-column name:

```clojure
;; Simple computed :by
(dt ds :by (fn [row] {:heavy? (> (:mass row) 4000)})
    :agg {:n nrow})

;; Custom bucketing function with friendly result column name
(defn percentile-bucket [col pct]
  (with-meta
    (fn [row]
      (let [v (get row col)]
        (when (some? v)
          (int (* pct (/ v 100))))))
    {:datajure/col (keyword (str (name col) "-pct-bucket"))}))

(dt ds :by [(percentile-bucket :score 10)] :agg {:n nrow})
;; Result column is named :score-pct-bucket
```

`xbar` uses the same mechanism internally. If no metadata is attached, result columns get synthetic names (`:fn-0`, `:fn-1`, ...).

## Rename

```clojure
(rename ds {:mass :weight-kg :species :penguin-species})
```

## Concise Namespace

Short aliases for power users (q / data.table refugees in particular):

```clojure
(require '[datajure.concise :refer [mn sm md sd ct nuniq fst lst wa ws mx mi N between]])

(dt ds :by [:species] :agg {:n N :avg #dt/e (mn :mass)})
```

| Symbol | Full name |
|--------|-----------|
| `mn`   | mean |
| `sm`   | sum |
| `md`   | median |
| `sd`   | stddev |
| `mx`   | max (column maximum) |
| `mi`   | min (column minimum) |
| `ct`   | element count |
| `nuniq`| count-distinct |
| `fst`  | first-val |
| `lst`  | last-val |
| `wa`   | wavg (weighted average) |
| `ws`   | wsum (weighted sum) |
| `N`    | row count (alias for `nrow`) |
| `standardize` | stat/stat-standardize |
| `demean`      | stat/stat-demean |
| `winsorize`   | stat/stat-winsorize |
| `between`     | positional range selector |

Both `nrow` (discoverable) and `N` (terse, q/data.table style) live in `datajure.core`; `N` is also re-exported from `datajure.concise`.

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
user=> (dt ds :by [:species] :agg {:n nrow})
;; => dataset...

user=> (dt datajure.core/*dt* :order-by [(desc :n)])
```

Enable in `.nrepl.edn`: `{:middleware [datajure.nrepl/wrap-dt]}`

## Error Messages

Structured `ex-info` with suggestions. All errors carry a `:dt/error` key in `ex-data` for programmatic dispatch.

**Unknown column — Damerau-Levenshtein suggestions catch transpositions:**

```clojure
(dt ds :set {:bmi #dt/e (/ :mass :hieght)})
;; => ExceptionInfo: Unknown column(s) #{:hieght} in :set :bmi expression
;;    Did you mean: :height (edit distance 1)
;;    Available: :species :year :mass :height :flipper
```

**Unknown op — namespace-aware suggestions at read time:**

```clojure
#dt/e (sqrt :x)
;; => ExceptionInfo: Unknown op `sqrt` in #dt/e expression. Did you mean: `sq`?

#dt/e (win/mvag :price 20)
;; => ExceptionInfo: Unknown op `win/mvag` in #dt/e expression. Did you mean: `win/mavg`?
```

**`:agg` plain-function footgun — detected and reported:**

```clojure
(dt ds :by [:species] :agg {:bad #(:mass %)})
;; => ExceptionInfo: :agg plain function for column :bad returned a column, not a scalar.
;;    In :agg, plain functions receive the group dataset, so `(:col %)` returns a column
;;    vector. Use `(dfn/mean (:col %))` or prefer `#dt/e (mn :col)` which handles both
;;    cases uniformly.
```

**Structural errors:**

```clojure
(dt ds :set {:a #dt/e (/ :x 1)} :agg {:n nrow})
;; => ExceptionInfo: Cannot combine :set and :agg. Use -> threading.

(dt ds :set {:bmi  #dt/e (/ :mass (sq :height))
             :obese #dt/e (> :bmi 30)})
;; => ExceptionInfo: Map-form :set cross-reference.
;;    :obese references #{:bmi}, which is being derived in the same map.
;;    Use vector-of-pairs [[:bmi ...] [:obese ...]] for sequential derivation.
```

## Evaluation Order

`dt` evaluates keywords in this fixed order, regardless of the order they appear in the call:

1. `:where` — filter rows
2. `:set` or `:agg` — derive or aggregate (mutually exclusive; see dispatch modes above)
3. `:select` — keep listed columns
4. `:order-by` — sort final output

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
| `datajure.core` | `dt`, `N`, `nrow`, `mean`, `sum`, `median`, `stddev`, `variance`, `max*`, `min*`, `count*`, `asc`, `desc`, `pass-nil`, `rename`, `xbar`, `cut`, `between`, `*dt*` |
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
2. **`:where` always filters** — conditional updates go inside `:set` via `if`/`cond`.
3. **Keyword lifting requires `#dt/e`** — no implicit magic in plain Clojure forms.
4. **Layered nil story** — nil literals are safe in `#dt/e`, aggregation helpers skip nils, `coalesce`/`div0`/`win/fills` handle the rest, `pass-nil` wraps plain functions. Not a blanket "nil-safe" claim, but a coherent set of rules that eliminate the common NPE footguns.
5. **Expressions are values** — `#dt/e` returns an AST, not a function. Store in vars, compose freely, build shared vocabularies.
6. **One function, not twenty-eight** — one `dt`, seven keywords, two expression modes. Threading for pipelines.
7. **Errors are data** — structured `ex-info` with `:dt/error` dispatch keys, Damerau-Levenshtein typo suggestions, extensible.
8. **Syntax layer, not engine** — delegate to tech.v3.dataset. Full interop with tablecloth, Clerk, Clay, and the Scicloj ecosystem.
9. **Steal the best ideas** — from data.table, q/kdb+, Polars, DataFramesMeta.jl, APL. The goal isn't to be any of them.

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

267 tests, 896 assertions (CI subset: 200 tests, 734 assertions).

## Prior Work

Datajure v1 was a routing layer across three backends (tablecloth, clojask, geni/Spark). v2 takes a different approach: a single, opinionated syntax layer directly on tech.v3.dataset, stealing good ideas from data.table (query form), q/kdb+ (time-series primitives), Polars (expressions as values), and DataFramesMeta.jl (one function, keyword arguments).

- v1 repo: https://github.com/clojure-finance/datajure/tree/v1

Special thanks to [YANG Ming-Tian](https://github.com/skylee03) for the original v1 implementation.

## License

Copyright © 2024–2026 Centre for Investment Management, HKU Business School.

Distributed under the Eclipse Public License version 2.0.
