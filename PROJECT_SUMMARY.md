# Datajure v2 — Project Summary

## Overview

Datajure v2 is a Clojure data manipulation library for finance and empirical research. It provides a clean, composable query DSL (`dt`) built directly on **tech.ml.dataset** (`tech.v3.dataset`). The core abstraction is a reader-tagged expression `#dt/e` that compiles column expressions to dataset operations. Datajure v2 is a **syntax layer**, not an engine — it sits above tech.v3.dataset exactly as dplyr/data.table sit above R's data frames.

**Status**: Early development. Syntax specification is finalized; no source files exist yet.

- v1 repo: https://github.com/clojure-finance/datajure
- v1 website: https://clojure-finance.github.io/datajure-website/

---

## Project Structure

```
datajure/
├── deps.edn                  # Project config
├── src/
│   └── datajure/
│       ├── core.clj          # dt query function, #dt/e registration (TO BUILD)
│       ├── expr.clj          # #dt/e reader tag + AST compiler (TO BUILD)
│       ├── join.clj          # join, cross-join, anti-join (TO BUILD)
│       ├── reshape.clj       # pivot-wider, pivot-longer / melt (TO BUILD)
│       ├── window.clj        # win/* window functions (TO BUILD)
│       ├── concise.clj       # short aliases: mn, sm, N, grp, gi, etc. (TO BUILD)
│       ├── io.clj            # unified file I/O wrapper (TO BUILD)
│       └── util.clj          # data cleaning utilities (TO BUILD)
└── test/
    └── datajure/             # (TO BUILD)
```

---

## Dependencies

| Dependency | Version | Role |
|---|---|---|
| `org.clojure/clojure` | 1.12.0 | Language runtime |
| `techascent/tech.ml.dataset` | 7.021 | Core dataset engine (columnar, JVM) |
| `nrepl/nrepl` (alias) | 1.3.1 | REPL server on port 7888 |

**Key architectural decision**: Built directly on `tech.v3.dataset`, NOT on `tablecloth`. Tablecloth is redundant — Datajure v2 independently provides everything tablecloth offers; adding it would mean fighting two layers of opinions.

---

## Core Syntax

### The `dt` function

```clojure
(dt ds
  :where    <predicate>
  :select   <columns>
  :set      <column-derivations>
  :agg      <aggregations>
  :by       <grouping-columns>
  :within-order <window-sort-spec>
  :order-by <sort-spec>)
```

All keywords optional. Result is always a `tech.v3.dataset`. `dt` is a **function**, not a macro.

### The `:by` × `:set`/`:agg` matrix (core design principle)

|            | No `:by`            | With `:by`          |
|------------|---------------------|---------------------|
| **`:set`** | Column derivation   | Window function     |
| **`:agg`** | Whole-table summary | Group aggregation   |

```clojure
;; Column derivation — add/update columns, keep all rows
(dt ds :set {:bmi #dt/e (/ :mass (sq :height))})

;; Group aggregation — collapse rows per group
(dt ds :by [:species] :agg {:n N :avg-mass #dt/e (mn :mass)})

;; Whole-table summary
(dt ds :agg {:total #dt/e (sm :mass) :n N})

;; Window function — compute within groups, keep all rows
(dt ds
  :by [:species]
  :within-order [(desc :mass)]
  :set {:rank  #dt/e (win/rank :mass)
        :cumul #dt/e (win/cumsum :mass)
        :prev  #dt/e (win/lag :mass 1)})
```

**Critical constraint**: `:set` + `:agg` in the same `dt` call is always an error. Use threading:
```clojure
(-> ds (dt :set {:bmi #dt/e (/ :mass (sq :height))})
       (dt :by [:species] :agg {:avg-bmi #dt/e (mn :bmi)}))
```

### Fixed evaluation order

1. `:where` — filter rows
2. `:set` OR `:agg` (mutually exclusive) — derive/window OR aggregate
3. `:select` — keep listed columns
4. `:order-by` — sort final output

Order is fixed regardless of keyword order in the call.

---

## The `#dt/e` Reader Tag

`#dt/e` is a reader tag that returns an AST object (not a compiled function). `dt` interprets the AST in context.

```clojure
;; With expression mode — terse, nil-safe, pre-validated
(dt ds :set {:bmi #dt/e (/ :mass (sq :height))})

;; Without — plain Clojure functions (always works, no pre-validation)
(dt ds :set {:bmi #(/ (:mass %) (sq (:height %)))})
```

**Keyword lifting**: bare keywords (`:mass`) auto-resolve to column vectors **only** inside `#dt/e`. Outside, they're just keywords.

### Auto-registration

`(require '[datajure.core])` merges `#dt/e` into `*data-readers*` via `alter-var-root` at load time — same pattern as `clojure.instant` / `#inst`. Side-effectful by design, documented in namespace docstring. Fallback: `(datajure/init!)` for AOT/script edge cases.

### Nil-safety rules (inside `#dt/e`)

1. **Comparisons** with nil → `false` (never throws)
2. **Arithmetic** with nil → nil propagates
3. **`coalesce`** → replace nil with fallback: `#dt/e (coalesce :mass 0)`
4. **`pass-nil`** → wrap plain functions: `(pass-nil #(parse-int (:x-str %)))`

All built-in aggregation helpers skip nil/NaN and return nil for all-missing inputs — never `0.0`, `-Infinity`, or `NaN`.

### Special forms recognized inside `#dt/e`

| Form | Compiles to |
|---|---|
| `(and ...)`, `(or ...)`, `(not ...)` | `dfn/and`, `dfn/or`, `dfn/not` (vectorized, not Clojure macros) |
| `(in :col #{...})` | Set membership test across column |
| `(between? :col lo hi)` | `(and (>= col lo) (<= col hi))`, nil → false |
| `(cond pred val ... :else val)` | Chain of vectorized ternary nodes |
| `(let [sym expr ...] body)` | Named intermediate AST nodes, evaluated once |
| `(win/rank :col)`, `(win/lag :col n)`, etc. | Window operations (validated: require `:by` + `:set`) |
| `(row/sum :a :b :c)`, etc. | Cross-column row-wise operations |
| `(coalesce :col fallback)` | nil replacement |
| `(count-distinct :col)` | Aggregation helper |

### Reusable expressions (first-class values)

```clojure
(def bmi #dt/e (/ :mass (sq :height)))
(dt ds :set {:bmi bmi})
(dt ds :where #dt/e (> bmi 30))   ; symbol resolved at compile time → AST splice
(dt ds :by [:species] :agg {:avg-bmi #dt/e (mn bmi)})
```

### AST metadata

Expression objects carry: referenced columns, operation types, nil-safety annotations, window function markers. Preserved during compilation for future query optimization (predicate/projection pushdown in v3).

---

## Window Functions (`win/*`)

Available: `win/rank`, `win/dense-rank`, `win/row-number`, `win/lag`, `win/lead`, `win/cumsum`, `win/cummin`, `win/cummax`, `win/cummean`, `win/rleid`

**Tie-breaking:**

| Function | Method | Example: [10,20,20,30] | SQL |
|---|---|---|---|
| `win/rank` | min | 1,2,2,4 | `RANK()` |
| `win/dense-rank` | dense | 1,2,2,3 | `DENSE_RANK()` |
| `win/row-number` | first (position) | 1,2,3,4 | `ROW_NUMBER()` |

**`win/rleid`**: increments when value changes — `[A A B B A A]` → `[1 1 2 2 3 3]`. Essential for regime/spell detection.

**`:within-order`** rules:
- Only valid in window mode (`:by` + `:set`). Error otherwise.
- Optional — when omitted, uses current row order within group.
- Controls ordering within each partition only; independent of `:order-by`.

---

## Row-wise Operations (`row/*`)

```clojure
#dt/e (row/sum :q1 :q2 :q3 :q4)       ; nil treated as 0
#dt/e (row/mean :q1 :q2 :q3 :q4)      ; skips nil
#dt/e (row/min :q1 :q2 :q3 :q4)       ; skips nil
#dt/e (row/max :q1 :q2 :q3 :q4)       ; skips nil
#dt/e (row/count-nil :q1 :q2 :q3 :q4) ; count missing
#dt/e (row/any-nil? :q1 :q2 :q3 :q4)  ; boolean
```

All return nil when every input is nil.

---

## `:select` — Polymorphic Column Selectors

```clojure
(dt ds :select [:species :year :mass])          ; explicit list
(dt ds :select :species)                         ; single column
(dt ds :select #"body-.*")                       ; regex on names
(dt ds :select :type/numerical)                  ; all numeric columns
(dt ds :select :!type/numerical)                 ; complement
(dt ds :select [:not :id :timestamp])            ; exclusion
(dt ds :select (complement #{:id :timestamp}))   ; set exclusion
(dt ds :select (fn [meta] (= :float64 (:datatype meta))))  ; predicate
(dt ds :select {:species :sp :mass :m})          ; rename-on-select
```

---

## `:set` / `:agg` — Map vs Vector-of-Pairs

**Map (simultaneous)**: all expressions see original columns only. Use for independent derivations.

**Vector-of-pairs (sequential)**: later entries can reference columns from earlier entries.

```clojure
;; Sequential: :obese can reference :bmi
(dt ds :set [[:bmi   #dt/e (/ :mass (sq :height))]
             [:obese #dt/e (> :bmi 30)]])
```

---

## Joins (`datajure.join`)

```clojure
(join X Y :on :id :how :left)
(join X Y :on :id :how :left :validate :m:1 :report true)
```

- `:how` — `:inner`, `:left`, `:right`, `:full`, `:anti`, `:semi`, `:cross`
- `:validate` — `:1:1`, `:1:m`, `:m:1`, `:m:m` — raises on cardinality violation
- `:report` — prints matched/left-only/right-only counts (like Stata's `_merge`)

---

## Reshape (`datajure.reshape`)

```clojure
(melt ds {:id [:species :year] :measure [:mass :flipper :bill]})
(pivot-wider ds :id-cols [:date :permno] :names-from :variable :values-from :value)
(pivot-longer ds [:ret1 :ret2 :ret3] :names-to :period :values-to :ret)
(rename ds {:mass :weight-kg})
```

---

## Namespace Architecture

### `datajure.core` — Full names
```clojure
(require '[datajure.core :as dt])
```
Aggregation helpers: `mean`, `sum`, `median`, `stddev`, `variance`, `min*`, `max*`, `count-distinct`, `N`

### `datajure.concise` — Opt-in short aliases
```clojure
(require '[datajure.concise :refer [mn sm md sd ct nuniq N grp gi]])
;; mn=mean, sm=sum, md=median, sd=stddev, ct=count, nuniq=count-distinct
;; N=row count, grp=group dataset (.SD equivalent), gi=group row indices (.GRP/.I)
```
`grp`/`gi`: v2.0 scope but deferred until priorities 1–10 are solid.

### `datajure.io` — Unified file I/O
```clojure
(require '[datajure.io :as dio])
(dio/read "data.csv")         ; dispatches on extension
(dio/read "data.parquet")
(dio/read-seq "huge.parquet") ; streaming → seq of datasets
(dio/write ds "output.parquet")
```
Formats: CSV, TSV, Parquet, Arrow, JSON, Excel. Gzipped variants auto-detected.

### `datajure.util` — Data cleaning
```clojure
(require '[datajure.util :as du])
(du/describe ds)                           ; summary stats → dataset (pipes with dt)
(du/clean-column-names ds)                 ; "Some Ugly Name!" → :some-ugly-name
(du/duplicate-rows ds [:id :date])         ; find dups by column subset
(du/mark-duplicates ds [:id :date])        ; adds :duplicate? boolean column
(du/drop-constant-columns ds)
(du/coerce-columns ds {:year :int64 :mass :float64})
```

---

## Backend Architecture

### Why tech.v3.dataset directly (not tablecloth)

Datajure v2 independently provides everything tablecloth offers. Adding tablecloth would mean fighting two competing opinion layers. Tablecloth is an alternative syntax peer, not a dependency.

### AST compilation strategy

```
#dt/e expression → AST (intermediate representation)
                       ↓
              compiler dispatch (protocol in v3)
              ↓              ↓              ↓
        tech.v3 (v2)    clojask (v3)   DuckDB (v3)
```

Each potential backend has a genuinely different execution model. v2.0 has one compiler; v3 adds protocol-based dispatch once AST is battle-tested.

**For larger-than-memory today**: use clojask directly → convert result to tech.v3 dataset → continue with Datajure.

---

## Error Messages

Structured `ex-info` with `ex-data`, dispatched via multimethod. Pre-execution column validation from `#dt/e` AST metadata.

```clojure
(dt ds :set {:bmi #dt/e (/ :mass :hieght)})
;; ExceptionInfo: Unknown column :hieght in :set expression
;;   Did you mean: :height (edit distance: 1)
;;   Available: :species :year :mass :height :flipper-length-mm
;;   (ex-data *e) => {:dt/error :unknown-column, :dt/column :hieght,
;;                    :dt/closest [:height], :dt/available [...]}
```

Errors extensible via `(defmulti explain-error :dt/error)`.

---

## REPL Conveniences

- **`*dt*`** — holds last dataset result (bound by nREPL middleware, not by `dt` itself — same pattern as `*1`)
- **Pretty-printing** — formatted tables in terminal, enhanced in CIDER/Calva/Clerk

---

## Implementation Priority

| # | Feature |
|---|---|
| 1 | `:by` × `:set`/`:agg` matrix |
| 2 | `#dt/e` expression mode (nil-safety, `coalesce`, `let`, `cond`, `and`/`or`/`not`/`in`, `between?`) |
| 3 | Polymorphic `:select` and `:by` |
| 4 | `pass-nil` wrapper |
| 5 | REPL-first: pretty-print + `*dt*` + Clerk |
| 6 | Structured error messages |
| 7 | `:within-order` for window ordering |
| 8 | `asc`/`desc` sort helpers |
| 9 | Reusable `#dt/e` expressions in vars |
| 10 | `win/*` functions inside `#dt/e` (incl. `win/rleid`) |
| 11 | `row/*` functions inside `#dt/e` |
| 12 | `count-distinct` aggregation helper |
| 13 | `datajure.concise` namespace |
| 14 | Vector-of-pairs sequential semantics |
| 15 | `datajure.io` — unified file I/O |
| 16 | `datajure.util` — `describe`, `clean-column-names`, `duplicate-rows` |
| 17 | Join `:validate` and `:report` |
| 18 | Expression composition (symbol → var resolution, v2.1) |
| 19 | `cut` for binning (v2.1) |
| 20 | `stat/*` namespace: `standardize`, `demean`, `winsorize` (v2.1) |
| 21 | `div0` nil-safe division (v2.1) |
| 22 | `between` column selector (v2.1) |
| 23 | Per-expression windowing (`over`, v3) |
| 24 | Backend-agnostic AST compilation (v3) |
| 25 | Lazy pipeline optimization (v3) |
| 26 | Rolling/asof joins (v3) |

---

## Design Principles

1. `dt` is a function — not a macro
2. `dt` is a query form — joins/reshape/IO are separate composable functions
3. `:where` always filters — conditional updates go inside `:set`
4. Keyword lifting requires `#dt/e` — no implicit magic outside expression mode
5. Nil-safe by default — four-rule nil handling baked into `#dt/e` compiler
6. Expressions are values — `#dt/e` returns a reusable AST with preserved metadata
7. Two expression modes, one choice point — `#dt/e` (vectorized) vs plain functions (flexible)
8. Polymorphic selectors — `:select` dispatch on argument type, not separate functions
9. Threading for pipelines — don't reinvent `->`, use it
10. Errors are data — structured `ex-info`, extensible multimethod
11. One function, not 28 — one `dt`, six keywords, two expression modes
12. One name per concept — no aliases in core
13. Two audiences — explicit core for learners, concise namespace for power users
14. Syntax layer, not engine — delegate to `tech.v3.dataset` and `dfn`

---

## Quick Reference

```clojure
;; Basic filter + select
(dt ds :where #dt/e (> :year 2008) :select [:species :mass])

;; Boolean / membership / range
(dt ds :where #dt/e (and (> :mass 4000) (not (= :species "Adelie"))))
(dt ds :where #dt/e (in :species #{"Gentoo" "Chinstrap"}))
(dt ds :where #dt/e (between? :year 2005 2010))

;; Polymorphic select
(dt ds :select #"body-.*")
(dt ds :select :type/numerical)
(dt ds :select [:not :id :timestamp])

;; Derive columns
(dt ds :set {:bmi #dt/e (/ :mass (sq :height))})

;; Sequential derivation (dependencies)
(dt ds :set [[:bmi   #dt/e (/ :mass (sq :height))]
             [:obese #dt/e (> :bmi 30)]])

;; Reusable expressions
(def bmi #dt/e (/ :mass (sq :height)))
(dt ds :set {:bmi bmi})
(dt ds :by [:species] :agg {:avg-bmi #dt/e (mn bmi)})

;; Group + aggregate
(dt ds :by [:species] :agg {:n N :avg #dt/e (mn :mass)})

;; Window functions
(dt ds :by [:species] :within-order [(desc :mass)]
    :set {:rank #dt/e (win/rank :mass)})

;; cond inside #dt/e
(dt ds :set {:cat #dt/e (cond (> :bmi 40) "severe"
                              (> :bmi 30) "obese"
                              :else       "normal")})

;; let inside #dt/e
(dt ds :set {:adj #dt/e (let [bmi (/ :mass (sq :height))
                               base (if (> :year 2010) 1.1 1.0)]
                           (* base bmi))})

;; Nil handling
(dt ds :where #dt/e (> :mass 4000))               ; nil → false
(dt ds :set {:mass #dt/e (coalesce :mass 0)})      ; nil → 0
(dt ds :set {:x (pass-nil #(parse-int (:x-str %)))}) ; nil → nil

;; Row-wise
(dt ds :set {:total #dt/e (row/sum :q1 :q2 :q3 :q4)
             :n-miss #dt/e (row/count-nil :q1 :q2 :q3 :q4)})

;; Full pipeline
(-> ds
    (dt :where #dt/e (> :year 2008)
        :by [:species]
        :agg {:avg-mass #dt/e (mn :mass)})
    (dt :order-by [(desc :avg-mass)])
    (dt :select [:species :avg-mass]))

;; HAVING equivalent
(-> ds
    (dt :by [:species] :agg {:n N})
    (dt :where #dt/e (>= :n 10)))

;; Top-N per group
(-> ds
    (dt :by [:species] :within-order [(desc :mass)]
        :set {:rank #dt/e (win/rank :mass)})
    (dt :where #dt/e (<= :rank 3)))

;; Joins
(-> (join X Y :on :id :how :left :validate :m:1)
    (dt :where #dt/e (> :year 2008)))

;; File I/O
(def ds (dio/read "data.parquet"))
(dio/write ds "output.csv")

;; Data cleaning
(-> (dio/read "messy.csv")
    (du/clean-column-names)
    (du/mark-duplicates [:id :date])
    (dt :where #dt/e (not :duplicate?)))

;; Interop with fastmath in :agg (plain functions receive group dataset)
(dt ds :by [:species]
    :agg {:skew #(fstats/skewness (:mass %))
          :corr #(fstats/correlation (:mass %) (:height %))})
```
