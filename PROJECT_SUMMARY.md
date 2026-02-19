# Datajure v2 — Project Summary

## Overview

Datajure v2 is a Clojure data manipulation library for finance and empirical research. It provides a clean, composable query DSL (`dt`) built directly on **tech.ml.dataset** (`tech.v3.dataset`). The core abstraction is a reader-tagged expression `#dt/e` that compiles column expressions to vectorized dataset operations. Datajure v2 is a **syntax layer**, not an engine — it sits above tech.v3.dataset exactly as dplyr/data.table sit above R's data frames.

- v1 repo: https://github.com/clojure-finance/datajure
- v1 website: https://clojure-finance.github.io/datajure-website/

---

## Implementation Status

| Phase | Description | Status |
|---|---|---|
| 1 | AST foundation (`datajure.expr`) | ✅ Complete |
| 2 | `dt` core, no grouping (`:where`, `:set`, `:agg`, `:select`, `:order-by`) | ✅ Complete |
| 3 | Grouping (`:by` + `:agg`/`:set`, conflict detection) | ✅ Complete |
| 4 | Wire up (evaluation order, `and`/`or`/`not`/`in`/`between?`) | ✅ Complete |
| 5 | Map `:set` simultaneous semantics, float division, column validation, test coverage | ✅ Complete |

---

## Project Structure

```
datajure/
├── deps.edn
├── PROJECT_SUMMARY.md
├── datajure-v2-syntax-revised.md   # Full syntax design spec
├── resources/
│   └── data_readers.clj          # {dt/e datajure.expr/read-expr}
├── src/
│   └── datajure/
│       ├── expr.clj              # ✅ AST + compiler + reader tag + col-refs
│       └── core.clj              # ✅ dt function + helpers (N, asc, desc) + column validation
│       ;; TO BUILD:
│       ;; join.clj, reshape.clj, window.clj, concise.clj, io.clj, util.clj
└── test/
    └── datajure/
        └── core_test.clj         # ✅ 28 tests, 50 assertions — full coverage
```

---

## Dependencies

| Dependency | Version | Role |
|---|---|---|
| `org.clojure/clojure` | 1.12.4 | Language runtime |
| `techascent/tech.ml.dataset` | 8.003 | Core dataset engine (columnar, JVM) |
| `nrepl/nrepl` (alias `:nrepl`) | 1.5.2 | REPL server on port 7888; also adds `test/` to classpath |

---

## `src/datajure/expr.clj` — AST + Compiler

### Public API

| Function | Description |
|---|---|
| `col-node [kw]` | `{:node/type :col :col/name kw}` |
| `lit-node [v]` | `{:node/type :lit :lit/value v}` |
| `op-node [op args]` | `{:node/type :op :op/name op :op/args args}` |
| `col-refs [node]` | Extract set of column keywords referenced by an AST node |
| `compile-expr [node]` | AST → `fn [ds] → column-vector-or-scalar` |
| `read-expr [form]` | Reader tag handler; called at read time |
| `register-reader! []` | AOT/script fallback via `alter-var-root` |

### Op table (sym → keyword → dfn fn)

| Source symbol | Op keyword | Implementation | Context |
|---|---|---|---|
| `+` `-` `*` `/` | `:+` `:-` `:*` `:div` | `dfn/+` `-` `*` `(fn [a b] (dfn// (dfn/double a) (dfn/double b)))` | Arithmetic |
| `sq` `log` | `:sq` `:log` | `dfn/sq` `dfn/log` | Arithmetic |
| `>` `<` `>=` `<=` `=` | `:>` `:<` `:>=` `:<=` `:=` | `dfn/>` etc., `dfn/eq` | Comparison |
| `and` `or` `not` | `:and` `:or` `:not` | `dfn/and` `dfn/or` `dfn/not` | Boolean |
| `mn` `sm` `md` `sd` | `:mn` `:sm` `:md` `:sd` | `dfn/mean` `dfn/sum` `dfn/median` `dfn/standard-deviation` | Aggregation |
| `in` | `:in` | `dtype/make-reader :boolean` + `contains?` | Membership |
| `between?` | `:between?` | `dfn/and (dfn/>= col lo) (dfn/<= col hi)` | Range |

### Why keywords for op names (not symbols)
The Clojure compiler walks literal data structures returned by reader tags and resolves symbols. `and`/`or`/`not` are macros — resolving them fails with "Can't take value of a macro". **Keywords are self-evaluating** and bypass this entirely. `sym->op` maps source symbols → keywords at read time; `op-table` maps keywords → dfn fns at compile time.

### Division always returns float
The `:div` op casts both arguments to double before dividing via `dfn/double`. This ensures `#dt/e (/ :mass 1000)` returns `3.75`, not `3` (integer truncation). This matches the expected behavior for data analysis.

### Nil-safety rules
- Comparison op with `nil` literal arg → `dtype/make-reader :boolean` all-false (0 rows selected)
- Arithmetic op with `nil` literal arg → `nil` (becomes missing in dataset)
- `in` / `between?` are in `comparison-ops` — nil args → 0 rows
- Missing values in dataset columns handled natively by `dfn`

### Critical implementation note: boolean mask type
`ds/select-rows` interprets a raw `boolean-array` as **row indices** (false=0, true=1), not a boolean mask. All boolean results from `compile-expr` must use `dtype/make-reader :boolean ...` to return a tech.v3 typed reader. Using `boolean-array` silently produces wrong row selections. This affected both the nil-safety path and the `:in` op — both fixed and covered by regression tests.

---

## `src/datajure/core.clj` — `dt` Query Function

### Public API

| Symbol | Type | Description |
|---|---|---|
| `dt` | fn | Main query function |
| `N` | var | Row count helper (`ds/row-count`); use as value in `:agg` maps |
| `asc [col]` | fn | Sort spec `{:order :asc :col col}`; use in `:order-by` |
| `desc [col]` | fn | Sort spec `{:order :desc :col col}`; use in `:order-by` |

### `dt` function signature

```clojure
(dt dataset
  :where    <predicate>
  :set      <derivations>
  :agg      <aggregations>
  :by       <grouping-columns>
  :select   <column-selector>
  :order-by <sort-specs>)
```

All keywords optional. Fixed evaluation order regardless of keyword order in call:
```
:where → :set (or :agg) → :select → :order-by
```

**Constraint:** `:set` and `:agg` are mutually exclusive in a single `dt` call. Combining them throws:
```
ExceptionInfo: Cannot combine :set and :agg in the same dt call.
               Use -> threading for multi-step queries.
{:dt/error :set-agg-conflict}
```

### Internal dispatch predicate

```clojure
(defn- expr-node? [x]
  (and (map? x) (contains? x :node/type)))
```

Used throughout to dispatch between `#dt/e` AST and plain fn.

---

## Keyword Reference

### `:by` × `:set`/`:agg` Matrix

|            | No `:by`              | With `:by`                        |
|------------|-----------------------|-----------------------------------|
| **`:set`** | Column derivation     | Window mode (all rows preserved)  |
| **`:agg`** | Whole-table summary   | Group aggregation (rows collapsed)|

### `:where` — Filter rows

```clojure
(dt ds :where #dt/e (> :mass 4000))
(dt ds :where #dt/e (and (> :mass 4000) (< :year 2010)))
(dt ds :where #dt/e (in :species #{"Gentoo" "Chinstrap"}))
(dt ds :where #dt/e (between? :year 2007 2009))
(dt ds :where #(when (:mass %) (> (:mass %) 4000)))   ;; plain fn — row map
```

### `:set` — Derive/update columns

```clojure
;; Map (simultaneous — all see original dataset)
(dt ds :set {:bmi     #dt/e (/ :mass (sq :height))
             :mass-kg #dt/e (/ :mass 1000)})

;; Vector-of-pairs (sequential — later entries see earlier results)
(dt ds :set [[:bmi   #dt/e (/ :mass (sq :height))]
             [:obese #dt/e (> :bmi 0.30)]])

;; Plain fn (receives row map, returns scalar)
(dt ds :set {:bmi #(/ (:mass %) (* (:height %) (:height %)))})
```

**Simultaneous vs sequential semantics:** Map `:set` evaluates all expressions against the original dataset — sibling entries cannot reference each other. Vector-of-pairs `:set` is sequential — later entries see columns created by earlier entries. The same applies to `:agg`.

### `:by` + `:set` — Window mode (all rows preserved)

```clojure
(dt ds :by [:species] :set {:mean-mass #dt/e (mn :mass)})

;; Sequential: demean within group
(dt ds :by [:species]
    :set [[:mean-mass #dt/e (mn :mass)]
          [:diff      #dt/e (- :mass :mean-mass)]])
```

### `:agg` — Whole-table summary

```clojure
(dt ds :agg {:n N :avg #dt/e (mn :mass) :sum #dt/e (sm :mass)})
(dt ds :agg {:avg #(dfn/mean (:mass %))})   ;; plain fn — receives whole dataset
```

### `:by` + `:agg` — Group aggregation (rows collapsed)

```clojure
(dt ds :by [:species] :agg {:n N :avg #dt/e (mn :mass)})
(dt ds :by [:species :year] :agg {:avg-mass #dt/e (mn :mass)})
```

### `:select` — Polymorphic column selection

```clojure
(dt ds :select [:species :mass])           ;; vector of keywords
(dt ds :select :species)                   ;; single keyword
(dt ds :select [:not :year :height])       ;; exclusion
(dt ds :select #"mass.*")                  ;; regex on column names
(dt ds :select #(not= % :year))            ;; predicate fn on column keyword
(dt ds :select {:species :sp :mass :m})    ;; map = select + rename
```

### `:order-by` — Sort rows

```clojure
(dt ds :order-by [(desc :mass)])
(dt ds :order-by [(asc :year) (desc :mass)])
(dt ds :order-by [:mass])                  ;; bare keyword = asc
```

---

## `#dt/e` Reader Tag

### Registration
`resources/data_readers.clj` → `{dt/e datajure.expr/read-expr}` loaded at JVM startup. `register-reader!` is an AOT/script fallback.

### AST node shapes
```clojure
{:node/type :col  :col/name  :mass}
{:node/type :lit  :lit/value 4000}
{:node/type :op   :op/name :> :op/args [...]}
```

---

## Internal Architecture

### Key private functions in `core.clj`

| Function | Purpose |
|---|---|
| `expr-node?` | Dispatch predicate: AST map vs plain fn |
| `validate-expr-cols` | Pre-execution column validation — extracts refs via `expr/col-refs`, checks against dataset |
| `apply-where` | `:where` handler — validates cols, AST → `ds/select-rows`, fn → `ds/filter` |
| `derive-column` | Single column derivation — validates cols, AST → compiled fn, fn → `mapv` over rows |
| `apply-set` | `:set` handler — map: simultaneous (all derive against original ds); vector: sequential via `reduce` |
| `apply-group-set` | `:by + :set` — `ds/group-by` → `apply-set` per group → `ds/concat` |
| `eval-agg` | Single agg evaluation — validates cols, AST → compiled fn, fn → direct call |
| `apply-agg` | Whole-table `:agg` — wraps scalars in `[val]` → `ds/->dataset` |
| `apply-group-agg` | `:by + :agg` — `ds/group-by` → per-group agg → `ds/concat`. Group-key values wrapped in vectors for consistent `ds/->dataset` input |
| `apply-select` | `:select` handler — polymorphic dispatch on selector type |
| `apply-order-by` | `:order-by` handler — builds `Comparator` via `reify` |

### Evaluation order in `dt` (fixed via `cond->` ordering)

```
1. :where
2a. :by + :set  →  apply-group-set   (window mode)
2b. :set        →  apply-set         (column derivation)
2c. :by + :agg  →  apply-group-agg   (group aggregation)
2d. :agg        →  apply-agg         (whole-table summary)
3. :select
4. :order-by
```

Steps 2a/2b and 2c/2d are mutually exclusive — `:set + :agg` throws `:set-agg-conflict` before any processing.

---

## Error Handling

Errors use `ex-info` with structured `ex-data` under `:dt/error` key:

| `:dt/error` | Condition | Message |
|---|---|---|
| `:set-agg-conflict` | `:set` and `:agg` in same `dt` call | "Cannot combine :set and :agg. Use -> threading." |
| `:unknown-column` | `#dt/e` references column not in dataset | "Unknown column(s) #{:foo} in :where expression" |

The `:unknown-column` error includes:
- `:dt/columns` — set of unknown column keywords
- `:dt/context` — where the error occurred (e.g. `:where`, `:set :bmi`, `:agg :avg`)
- `:dt/available` — sorted vector of available column names

Future: extensible `explain-error` multimethod dispatching on `:dt/error`, Levenshtein suggestions for typos.

---

## Tests

`test/datajure/core_test.clj` — 28 tests, 50 assertions. Run via REPL (`:nrepl` alias puts `test/` on classpath):

```clojure
(require '[datajure.core-test])
(clojure.test/run-tests 'datajure.core-test)
```

Or via `load-file` if `test/` is not on classpath:

```clojure
(load-file "test/datajure/core_test.clj")
(clojure.test/run-tests 'datajure.core-test)
```

### Test coverage

| Category | Tests |
|---|---|
| **Regression: boolean mask** | `in-returns-correct-rows`, `in-nil-set-returns-zero-rows`, `nil-safety-comparison-returns-zero-rows`, `between-nil-bound-returns-zero-rows` |
| **`:in` / `:between?` correctness** | `in-correctness`, `between-correctness`, `in-combined-with-and` |
| **`:by + :agg`** | `group-agg-basic`, `group-agg-multi-column-by` |
| **`:by + :set` (window)** | `group-set-window-mode`, `group-set-sequential-demean` |
| **`:order-by`** | `order-by-desc`, `order-by-multi-key` |
| **`:select` (all 6 variants)** | `select-vector`, `select-single-keyword`, `select-not`, `select-regex`, `select-predicate`, `select-rename-map` |
| **Plain fn paths** | `plain-fn-where`, `plain-fn-set`, `plain-fn-agg` |
| **`:set` semantics** | `set-agg-conflict`, `map-set-simultaneous-semantics`, `vector-set-sequential-semantics` |
| **Whole-table agg** | `whole-table-agg` |
| **Threading** | `threading-pipeline` |
| **Column validation** | `unknown-column-validation` |

### Regression tests (critical — must not regress)

Root cause of the original four regression tests: `boolean-array` is interpreted by `ds/select-rows` as row *indices* (false=0, true=1), not a boolean mask. Fixed by using `dtype/make-reader :boolean ...` throughout `compile-expr`.

---

## Complete Usage Examples

```clojure
(require '[datajure.core :as core])

;; Filter — all boolean ops
(core/dt ds :where #dt/e (> :mass 4000))
(core/dt ds :where #dt/e (and (> :mass 4000) (not (= :species "Adelie"))))
(core/dt ds :where #dt/e (in :species #{"Gentoo" "Chinstrap"}))
(core/dt ds :where #dt/e (between? :year 2007 2009))

;; Derive + filter + select + sort
(core/dt ds
  :where    #dt/e (> :year 2007)
  :set      {:bmi #dt/e (/ :mass (sq :height))}
  :select   [:species :mass :bmi]
  :order-by [(core/desc :mass)])

;; Sequential derivation
(core/dt ds :set [[:bmi   #dt/e (/ :mass (sq :height))]
                  [:obese #dt/e (> :bmi 0.30)]])

;; Aggregate whole table
(core/dt ds :agg {:n core/N :avg #dt/e (mn :mass) :total #dt/e (sm :mass)})

;; Group aggregation
(core/dt ds :by [:species] :agg {:n core/N :avg #dt/e (mn :mass)})

;; Window mode — all rows preserved, group stats added
(core/dt ds :by [:species] :set {:mean-mass #dt/e (mn :mass)})

;; Window + sequential: demean within group
(core/dt ds :by [:species]
    :set [[:mean-mass #dt/e (mn :mass)]
          [:diff      #dt/e (- :mass :mean-mass)]])

;; :set + :agg conflict — throws ex-info
(core/dt ds :set {:x #dt/e (/ :mass 1000)} :agg {:n core/N})
;; => ExceptionInfo {:dt/error :set-agg-conflict}

;; Threaded pipeline
(-> ds
    (core/dt :where #dt/e (> :year 2007))
    (core/dt :by [:species] :agg {:n core/N :avg #dt/e (mn :mass)})
    (core/dt :order-by [(core/desc :avg)]))

;; Post-agg filter (HAVING equivalent)
(-> ds
    (core/dt :by [:species] :agg {:n core/N :avg #dt/e (mn :mass)})
    (core/dt :where #dt/e (>= :n 10)))
```

---

## Remaining Spec Features (Not Yet Implemented)

### v2.0 scope — next steps (suggested order)

| Priority | Feature | Complexity | Notes |
|---|---|---|---|
| 1 | `if` / `cond` / `let` special forms in `#dt/e` | Medium | `if` is the base case; `cond` compiles to nested `if`; `let` adds local bindings. Requires special-casing in `parse-form` before the generic `(seq? form)` branch. |
| 2 | `coalesce` in `#dt/e` | Small | First non-nil across columns or literals. SQL semantics. New op in `op-table`. |
| 3 | `count-distinct` agg helper | Small | New op: `(count-distinct :species)`. Trivial addition to `op-table`. |
| 4 | `pass-nil` wrapper fn | Small | Higher-order fn for plain fns: nil input → nil output, no crash. |
| 5 | `:select` type selectors | Small | `:type/numerical`, `:!type/numerical` — dispatch on column dtype metadata in `apply-select`. |
| 6 | Levenshtein suggestions in column validation | Small | Enhance existing `:unknown-column` error with closest-match suggestions. |
| 7 | `:by` with computed grouping fn | Small | Already partially works via `ds/group-by`; needs dispatch in `apply-group-agg`/`apply-group-set`. |
| 8 | `:within-order` for window mode | Medium | Sort within each partition before window computation. Only valid with `:by + :set`. |
| 9 | Standalone `rename` fn | Trivial | `(rename ds {:mass :weight-kg})` — without dropping columns. |
| 10 | `datajure.concise` namespace | Small | Short aliases: `mn sm md sd ct nuniq N grp gi`. Opt-in require. |

### Larger standalone pieces (own namespaces)

| Namespace | Contents |
|---|---|
| `datajure.window` | `win/rank`, `win/dense-rank`, `win/row-number`, `win/lag`, `win/lead`, `win/cumsum`, `win/rleid`, etc. Used inside `#dt/e` in window mode. |
| `datajure.row` | `row/sum`, `row/mean`, `row/min`, `row/max`, `row/count-nil`, `row/any-nil?` — cross-column row-wise ops inside `#dt/e`. |
| `datajure.join` | `join` with `:on`, `:how`, `:validate` (cardinality check), `:report` (merge diagnostics). |
| `datajure.reshape` | `melt` for wide→long reshaping. |
| `datajure.io` | Unified `read`/`write` dispatching on file extension: CSV, Parquet, Arrow, Excel, TSV, gzip variants. |
| `datajure.util` | `describe`, `clean-column-names`, `duplicate-rows`, `mark-duplicates`, `drop-constant-columns`, `coerce-columns`. |
| `datajure.clerk` | Rich notebook rendering, interactive table display. |

### REPL / tooling

| Feature | Notes |
|---|---|
| `*dt*` last-result binding | Bound by nREPL middleware, not by `dt` itself. Mirrors Clojure's `*1`. |
| Rich pretty-printing | Formatted tables in terminal; enhanced in CIDER/Calva/Clerk. |

### v3 / deferred

| Feature | Notes |
|---|---|
| AST metadata | Track referenced columns, return type, op types — for future optimizers. |
| Backend-agnostic compilation | Compile same `#dt/e` AST to clojask / geni-Spark / DuckDB. |
| Lazy pipeline optimization | Predicate/projection pushdown across threaded `dt` calls. |
| Rolling/asof joins | `(join X Y :on :date :roll true)` |
| Per-expression windowing (`over`) | `#dt/e (over (mn :attack) [:type1])` — if uniform `:by` proves too restrictive. |

---

## Bugs Fixed (This Session)

| Bug | Description | Fix |
|---|---|---|
| Map `:set` sequential leak | Map `:set` used `reduce` over accumulating dataset, allowing sibling entries to see each other's results. Violated spec's simultaneous semantics. | Map branch now evaluates all expressions against original dataset, then merges results. Vector-of-pairs retains sequential `reduce`. |
| `apply-group-agg` mixed scalar/vector | Group-key values were bare scalars while agg values were wrapped in vectors. Relied on implicit `ds/->dataset` coercion. | Group-key values now wrapped with `update-vals group-key vector` before merge. |
| Integer division in `#dt/e` | `#dt/e (/ :mass 1000)` truncated to integer when both inputs were longs. | `:div` op now casts both args to double via `dfn/double` before dividing. |
| Missing column silent nil | `#dt/e` referencing a non-existent column returned nil (swallowed by nil-safety), producing wrong results silently. | Added `col-refs` to extract column references from AST. Added `validate-expr-cols` in `core.clj` that checks all `#dt/e` column refs against the dataset before execution. Throws `:dt/error :unknown-column` with context and available columns. |
