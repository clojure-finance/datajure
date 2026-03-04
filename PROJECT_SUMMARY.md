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
| 6 | `if` / `cond` / `let` special forms in `#dt/e` | ✅ Complete |
| 7 | `coalesce` special form in `#dt/e` | ✅ Complete |
| 8 | `count-distinct` aggregation helper | ✅ Complete |
| 9 | `pass-nil` wrapper for plain fns | ✅ Complete |
| 10 | `:select` type selectors (`:type/numerical`, `:!type/numerical`) | ✅ Complete |
| 11 | Levenshtein suggestions in column validation errors | ✅ Complete |
| 12 | `:by` with computed grouping fn (plain fn of row → map) | ✅ Complete |
| 13 | `:within-order` for window mode — parse/dispatch + validation | ✅ Complete |
| 14 | `:within-order` for window mode — intra-partition sorting | ✅ Complete |
| 15 | Standalone `rename` fn | ✅ Complete |
| 16 | `datajure.concise` namespace | ✅ Complete |
| 17a | Window functions — AST node type (`:win`), parser integration (`win/*` symbols), `win-refs` extraction | ✅ Complete |
| 17b | Window functions — compile-time validation (`win/*` only in `:set` context, blocked in `:where`/`:agg`) | ✅ Complete |
| 17c | Window functions — runtime implementations + compiler wiring | ✅ Complete |
| 18 | Reusable `#dt/e` expressions — composition via `:lit` node detection in `compile-expr`, `col-refs`, `win-refs` | ✅ Complete |
| 19 | Row-wise functions (`datajure.row`) — AST node type (`:row`), parser integration (`row/*` symbols), runtime implementations | ✅ Complete |
| 20a | `datajure.util` — `describe`, `clean-column-names`, `duplicate-rows`, `mark-duplicates`, `drop-constant-columns`, `coerce-columns` | ✅ Complete |
| 20b | `datajure.util` tests — 15 tests covering all 6 utility functions | ✅ Complete |
| 21 | Map `:set` cross-reference error — detect sibling column refs in map-form `:set`, suggest vector-of-pairs | ✅ Complete |
| 22 | One-time REPL info notes — `:agg` without `:by`, window mode activation, window mode without `:within-order` | ✅ Complete |
| 23 | `datajure.io` — unified `read`/`write`/`read-seq` dispatching on file extension | ✅ Complete |
| 24 | `datajure.reshape` — `melt` for wide→long | ✅ Complete |
| 25a | `datajure.join` — basic `join` with `:on`, `:how` (`:inner`, `:left`, `:right`, `:outer`), `:left-on`/`:right-on` | ✅ Complete |
| 25b | `datajure.join` — `:validate` cardinality checks (`:1:1`, `:1:m`, `:m:1`, `:m:m`) | ✅ Complete |
| 25c | `datajure.join` — `:report` merge diagnostics (matched/left-only/right-only counts) | ✅ Complete |
| 25d | `datajure.join` — comprehensive test coverage (31 tests, 55 assertions) | ✅ Complete |
| 26 | `*dt*` REPL binding — `core/*dt*` dynamic var + `datajure.nrepl/wrap-dt` middleware | ✅ Complete |
| 27 | `datajure.clerk` — Rich Clerk notebook viewers (dataset, expr, describe) + sample notebook | ✅ Complete |
| 28 | `datajure.clay` — Clay/Kindly notebook integration (view, view-expr, view-describe, install!, advisor) + sample notebook | ✅ Complete |
| 29 | Whole-dataset window mode — `win/*` and `:within-order` without `:by`, `apply-window-set`, `derivations-have-win?` | ✅ Complete |
| 30 | Adjacent-element ops — `win/delta`, `win/ratio`, `win/differ` | ✅ Complete |
| 31 | Rolling window functions — `win/mavg`, `win/msum`, `win/mdev`, `win/mmin`, `win/mmax` | ✅ Complete |
| 32 | EMA + forward-fill — `win/ema` (period/alpha dispatch), `win/fills` (forward-fill nulls) | ✅ Complete |
| 33 | Aggregation helpers — `first-val`, `last-val`, `wavg`, `wsum` in `#dt/e` op table | ✅ Complete |
| 34 | Concise aliases — `fst`, `lst`, `wa`, `ws` in `datajure.concise` | ✅ Complete |
| 35 | `div0` — nil-safe division (zero or nil denominator → nil) | ✅ Complete |
| 36 | `win/scan` — generalized cumulative scan (`:scan` AST node, `scan-op-table` for `+`/`*`/`max`/`min`, compiler wiring, `ast->string` support) | ✅ Complete |
| 37a | `xbar` — AST node (`:xbar`), `parse-form` dispatch, `col-refs`/`win-refs` traversal | ✅ Complete |
| 37b | `xbar` — `xbar-bucket` runtime helper + `:xbar` case in `compile-expr` (numeric + temporal unit dispatch via `condp`) | ✅ Complete |
| 37c | `xbar` — standalone `xbar` fn in `core.clj` for use in computed `:by` | ✅ Complete |
| 37d | `xbar` — tests in `core_test.clj` (numeric bucketing, temporal units, `:by` usage, edge cases) | ✅ Complete |
| 38 | `datajure.concise` — `mx` (alias for `core/max*`) and `mi` (alias for `core/min*`) + tests | ✅ Complete |
| 39 | `datajure.core` — full-name aggregation helpers: `mean`, `sum`, `median`, `stddev`, `variance`, `max*`, `min*` (aliases to `dfn`), `count*` (non-nil count); `concise` `mn`/`sm`/`md`/`sd`/`mx`/`mi` now alias through `core` + tests | ✅ Complete |
| 40 | `cut` — equal-count (quantile) binning: AST node (`:cut`), `parse-form` dispatch, `col-refs`/`win-refs` traversal, `cut-bucket` runtime (percentile breakpoints + binarySearch), `compile-expr` wiring, `ast->string` in `clerk.clj`, standalone `cut` fn in `core.clj` (throws with guidance), tests (AST parsing, col-refs, basic binning, nil handling, column validation, standalone error, use in `:where`) | ✅ Complete |
| 41 | `cut` `:from` option — compute breakpoints from a filtered reference subpopulation: `parse-form` detects `(cut :col n :from <pred-expr>)` where `:from` accepts any `#dt/e` expression (predicate or column keyword) as a boolean mask; `cut-bucket` 3-arity filters the binned column by the mask to get the reference population; covers the NYSE-breakpoints pattern: `(cut :mktcap 5 :from (= :exchcd 1))` computes quintile breakpoints from NYSE stocks only and applies them to all stocks. Tests: AST parsing (`:from` is `:op` node for predicate, `:col` node for bare keyword), col-refs traversal, NYSE-style binning, boolean column selector, nil in binned col, column validation on `:from` predicate. | ✅ Complete |

**Current test coverage: 209 tests, 693 assertions — all passing.**

---

## Project Structure

```
datajure/
├── deps.edn
├── PROJECT_SUMMARY.md
├── datajure-v2-syntax-revised.md   # Full syntax design spec
├── resources/
│   └── data_readers.clj            # {dt/e datajure.expr/read-expr}
├── src/
│   └── datajure/
│       ├── expr.clj                # ✅ AST + compiler + reader tag + col-refs + win-refs + win/row compilation + expr composition
│       ├── core.clj                # ✅ dt + helpers (N, asc, desc, pass-nil, rename, reset-notes!, xbar) + full-name agg helpers (mean, sum, median, stddev, variance, max*, min*, count*) + validation + info notes + whole-dataset window mode
│       ├── concise.clj             # ✅ Short aliases (mn sm md sd ct nuniq mx mi fst lst wa ws N dt asc desc rename pass-nil) — mn/sm/md/sd/mx/mi alias through core
│       ├── window.clj              # ✅ Window function implementations (rank, dense-rank, row-number, lag, lead, cumsum, cummin, cummax, cummean, rleid, delta, ratio, differ, mavg, msum, mdev, mmin, mmax, ema, fills)
│       ├── row.clj                 # ✅ Row-wise function implementations (row-sum, row-mean, row-min, row-max, row-count-nil, row-any-nil?)
│       ├── util.clj                # ✅ Data cleaning utilities (describe, clean-column-names, duplicate-rows, mark-duplicates, drop-constant-columns, coerce-columns)
│       ├── io.clj                  # ✅ Unified read/write/read-seq dispatching on file extension
│       ├── reshape.clj             # ✅ melt for wide→long reshaping
│       ├── join.clj                # ✅ join with :on, :how, :left-on/:right-on, :validate, :report
│       ├── nrepl.clj              # ✅ nREPL middleware for *dt* auto-binding
│       ├── clerk.clj              # ✅ Rich Clerk viewers (dataset, expr, describe) + install!
│       └── clay.clj               # ✅ Clay/Kindly integration (view, view-expr, view-describe, install!, advisor)
├── notebooks/
│   ├── datajure_demo.clj          # Sample Clerk notebook demonstrating all viewers
│   └── datajure_clay_demo.clj     # Sample Clay notebook demonstrating Kindly integration
└── test/
    └── datajure/
        ├── core_test.clj           # ✅ 98 tests, 450 assertions
        ├── concise_test.clj        # ✅ 5 tests, 18 assertions
        ├── util_test.clj           # ✅ 15 tests, 37 assertions
        ├── io_test.clj             # ✅ 10 tests, 16 assertions
        ├── reshape_test.clj        # ✅ 8 tests, 13 assertions
        ├── join_test.clj           # ✅ 31 tests, 55 assertions
        ├── nrepl_test.clj          # ✅ 10 tests, 22 assertions
        ├── clerk_test.clj          # ✅ 19 tests, 49 assertions
        └── clay_test.clj           # ✅ 13 tests, 33 assertions
```

---

## Dependencies

| Dependency | Version | Role |
|---|---|---|
| `org.clojure/clojure` | 1.12.4 | Language runtime |
| `techascent/tech.ml.dataset` | 8.003 | Core dataset engine (columnar, JVM) |
| `nrepl/nrepl` (alias `:nrepl`) | 1.5.2 | REPL server on port 7888; also adds `test/` to classpath |
| `io.github.nextjournal/clerk` (alias `:clerk`) | 0.18.1158 | Optional: notebook rendering; also adds `notebooks/` to classpath |
| `org.scicloj/clay` (alias `:clay`) | 2-beta56 | Optional: Clay/Kindly notebook rendering; also adds `notebooks/` to classpath |

---

## `src/datajure/expr.clj` — AST + Compiler

### Public API

| Function | Description |
|---|---|
| `expr-node? [x]` | Returns true if x is a `#dt/e` AST node (map with `:node/type`) |
| `col-node [kw]` | `{:node/type :col :col/name kw}` |
| `lit-node [v]` | `{:node/type :lit :lit/value v}` |
| `op-node [op args]` | `{:node/type :op :op/name op :op/args args}` |
| `win-node [win-op args]` | `{:node/type :win :win/op win-op :win/args args}` |
| `row-node [row-op args]` | `{:node/type :row :row/op row-op :row/args args}` |
| `col-refs [node]` | Extract set of column keywords referenced by an AST node (traverses all node types incl. `:win` and composed `:lit` nodes) |
| `win-refs [node]` | Extract set of window op keywords (e.g. `:win/rank`) referenced by an AST node (traverses composed `:lit` nodes) |
| `compile-expr [node]` | AST → `fn [ds] → column-vector-or-scalar` (recursively compiles composed expr-nodes found in `:lit` values) |
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
| `count-distinct` | `:nuniq` | `(fn [col] (count (distinct (dtype/->reader col))))` | Aggregation |
| `first-val` | `:first-val` | `(fn [col] (first (dtype/->reader col)))` | Aggregation |
| `last-val` | `:last-val` | `(fn [col] (nth r (dec (ecount r))))` | Aggregation |
| `wavg` | `:wavg` | `(fn [w v] weighted-avg, skips nil pairs)` | Aggregation (2-arg) |
| `wsum` | `:wsum` | `(fn [w v] weighted-sum, skips nil pairs)` | Aggregation (2-arg) |
| `div0` | `:div0` | `(fn [a b] nil if b is zero or nil, else float a/b)` | Arithmetic (nil-safe) |
| `in` | `:in` | `dtype/make-reader :boolean` + `contains?` | Membership |
| `between?` | `:between?` | `dfn/and (dfn/>= col lo) (dfn/<= col hi)` | Range |

### Window op table (sym → keyword → window fn)

| Source symbol | Op keyword | Implementation | Args |
|---|---|---|---|
| `win/rank` | `:win/rank` | `win/win-rank` | `[col]` |
| `win/dense-rank` | `:win/dense-rank` | `win/win-dense-rank` | `[col]` |
| `win/row-number` | `:win/row-number` | `win/win-row-number` | `[col]` |
| `win/lag` | `:win/lag` | `win/win-lag` | `[col offset]` |
| `win/lead` | `:win/lead` | `win/win-lead` | `[col offset]` |
| `win/cumsum` | `:win/cumsum` | `win/win-cumsum` | `[col]` |
| `win/cummin` | `:win/cummin` | `win/win-cummin` | `[col]` |
| `win/cummax` | `:win/cummax` | `win/win-cummax` | `[col]` |
| `win/cummean` | `:win/cummean` | `win/win-cummean` | `[col]` |
| `win/rleid` | `:win/rleid` | `win/win-rleid` | `[col]` |
| `win/delta` | `:win/delta` | `win/win-delta` | `[col]` |
| `win/ratio` | `:win/ratio` | `win/win-ratio` | `[col]` |
| `win/differ` | `:win/differ` | `win/win-differ` | `[col]` |
| `win/mavg` | `:win/mavg` | `win/win-mavg` | `[col width]` |
| `win/msum` | `:win/msum` | `win/win-msum` | `[col width]` |
| `win/mdev` | `:win/mdev` | `win/win-mdev` | `[col width]` |
| `win/mmin` | `:win/mmin` | `win/win-mmin` | `[col width]` |
| `win/mmax` | `:win/mmax` | `win/win-mmax` | `[col width]` |
| `win/ema` | `:win/ema` | `win/win-ema` | `[col period-or-alpha]` |
| `win/fills` | `:win/fills` | `win/win-fills` | `[col]` |
| `win/scan` | `:scan` (`:scan` AST node, not `:win`) | `win/win-scan` | `[op-sym col-expr]` |

### Row op table (sym → keyword → row fn)

| Source symbol | Op keyword | Implementation | Args |
|---|---|---|---|
| `row/sum` | `:row/sum` | `row/row-sum` | `[& cols]` |
| `row/mean` | `:row/mean` | `row/row-mean` | `[& cols]` |
| `row/min` | `:row/min` | `row/row-min` | `[& cols]` |
| `row/max` | `:row/max` | `row/row-max` | `[& cols]` |
| `row/count-nil` | `:row/count-nil` | `row/row-count-nil` | `[& cols]` |
| `row/any-nil?` | `:row/any-nil?` | `row/row-any-nil?` | `[& cols]` |

### Special forms in `parse-form`

| Special form | AST node type | Description |
|---|---|---|
| `(if pred then else?)` | `:if` | Element-wise ternary. `else` defaults to `nil`. |
| `(cond t1 v1 ... :else d)` | desugared to nested `:if` | Compiled to nested `:if` nodes at parse time. |
| `(let [x expr ...] body)` | `:let` | Sequential local bindings. Binding names stored as keywords. |
| `(coalesce arg1 arg2 ...)` | `:coalesce` | First non-nil value per row. SQL semantics. |
| `(win/* col ...)` | `:win` | Window function. Detected via `win-sym->op` before general op dispatch. |
| `(win/scan op col-expr)` | `:scan` | Generalized cumulative scan. First arg is a bare op symbol (`+`, `*`, `max`, `min`), converted to keyword. Uses separate `:scan` node type (not `:win`) because first arg is an operator, not a column. |
| `(row/* col1 col2 ...)` | `:row` | Row-wise function. Detected via `row-sym->op` after `win/*`, before general op dispatch. |
| `(xbar :col width)` / `(xbar :col width :unit)` | `:xbar` | Floor-division bucketing. `width` is a literal integer; `unit` is optional keyword (`:seconds`, `:minutes`, `:hours`, `:days`, `:weeks`) for temporal columns. Detected before general op dispatch. |
| `(cut :col n)` / `(cut :col n :from <pred-expr>)` | `:cut` | Equal-count (quantile) binning. `n` bins; optional `:from` accepts any `#dt/e` boolean expression as a reference population mask. The mask selects a **subset of rows** (potentially much smaller than the full dataset) to compute breakpoints from, which are then applied to **all rows**. This directly models the NYSE-breakpoints pattern: `(cut :mktcap 5 :from (= :exchcd 1))` computes quintile breakpoints from NYSE stocks only, then bins all stocks (NYSE + AMEX + NASDAQ) using those breakpoints — the reference population and the binned population are different sizes. |

### AST node shapes

```clojure
{:node/type :col  :col/name  :mass}
{:node/type :lit  :lit/value 4000}
{:node/type :op   :op/name :> :op/args [...]}
{:node/type :win  :win/op :win/rank :win/args [...]}
{:node/type :scan :scan/op :+ :scan/arg <node>}
{:node/type :row  :row/op :row/sum :row/args [...]}
{:node/type :if   :if/pred <node> :if/then <node> :if/else <node>}
{:node/type :let  :let/bindings [{:binding/name :kw :binding/expr <node>} ...]
                  :let/body <node>}
{:node/type :binding-ref  :binding-ref/name :kw}
{:node/type :coalesce  :coalesce/args [<node> ...]}
{:node/type :xbar     :xbar/col <node> :xbar/width <node> :xbar/unit <keyword-or-nil>}
{:node/type :cut      :cut/col <node> :cut/n <node> :cut/from <node-or-nil>}
```

### `parse-form` dispatch order
1. `(keyword? form)` → `:col` node
2. `(symbol in env?)` → `:binding-ref` node (let body references)
3. `(seq? form)` → special form dispatch: `if`, `cond`, `coalesce`, `let`, `xbar`, `cut`, then `win-sym->op` check, then `row-sym->op` check, then `:op` via `sym->op`
4. else → `:lit` node

### Expression composition mechanism

`#dt/e` expressions stored in vars can be composed inside other `#dt/e` expressions:

```clojure
(def bmi #dt/e (/ :mass (sq :height)))
(dt ds :by [:species] :agg {:avg-bmi #dt/e (mn bmi)})
```

**How it works:** `#dt/e` is a reader tag that returns an AST map (plain data). When Clojure evaluates `#dt/e (mn bmi)`, the reader tag calls `parse-form` which sees `bmi` as an unrecognized symbol and creates a `:lit` node. Clojure's compiler then evaluates the returned map literal, resolving the symbol `bmi` to its value — the AST map from the `def`. The result is a `:lit` node whose `:lit/value` is itself an expr-node.

Three functions detect this pattern:
- **`compile-expr`** `:lit` case: if `:lit/value` is an `expr-node?`, recursively compile it instead of returning it as a scalar.
- **`col-refs`** `:lit` case: if `:lit/value` is an `expr-node?`, recursively extract column references.
- **`win-refs`** `:lit` case: if `:lit/value` is an `expr-node?`, recursively extract window references.

No new AST node type was needed — Clojure's own evaluation semantics handle var resolution naturally.

### Key implementation notes

- **Op names are keywords** (not symbols) — avoids Clojure compiler resolving macros (`and`/`or`/`not`) in literal data structures returned by reader tags.
- **Let binding names are keywords** — bare symbols in AST maps cause "Unable to resolve symbol"; keywords are self-evaluating.
- **`compile-expr` is 2-arity**: `(compile-expr node)` and `(compile-expr node env)` where `env` is `keyword → value` for let bindings.
- **`:if` compile handles scalar pred**: `nil?`/`true?`/`false?` dispatch before `dtype/make-reader` to avoid `(nth true idx)` crash.
- **Division always returns float**: `:div` casts both args via `dfn/double`.
- **Boolean mask type**: All boolean results use `dtype/make-reader :boolean` — raw `boolean-array` is interpreted as row indices by `ds/select-rows`.
- **Nil-safety**: comparison with `nil` literal → all-false reader; arithmetic with `nil` → `nil`.
- **Window compilation**: `:win` nodes compile to `(fn [ds] (apply win-fn (map arg-fn args)))` — the window runtime fn receives evaluated column vectors from the dataset, same as regular ops.
- **Row compilation**: `:row` nodes compile identically to `:win` nodes — `(fn [ds] (apply row-fn (map arg-fn args)))`. Row functions take multiple columns and operate across them per row, returning a column of the same length.
- **Expression composition**: `:lit` nodes containing expr-nodes are recursively compiled/traversed — enables `#dt/e (mn bmi)` where `bmi` is a var holding another `#dt/e` AST.
- **`xbar` temporal dispatch uses `condp` not `case`**: `dtype-dt/milliseconds-in-*` are Vars (not compile-time constants), so `case` triggers an `ArrayIndexOutOfBoundsException` in the Clojure compiler when used as result values. `condp = unit` is the correct pattern.

---

## `src/datajure/window.clj` — Window Function Implementations

Each function takes a column (dtype reader/vector) and returns a column of the same length. Called per-partition by the expr compiler when processing `:win` AST nodes in window mode (`:by` + `:set` for partitioned, or `:set` alone for whole-dataset).

### Functions

| Function | Description | Nil handling |
|---|---|---|
| `win-rank [col]` | SQL RANK(): 1-based, min tie method, by current row order | N/A |
| `win-dense-rank [col]` | SQL DENSE_RANK(): 1-based, dense ties, no gaps | N/A |
| `win-row-number [col]` | SQL ROW_NUMBER(): 1-based sequential by position | N/A |
| `win-lag [col offset]` | Value `offset` rows back, nil for early rows | Returns nil |
| `win-lead [col offset]` | Value `offset` rows ahead, nil for late rows | Returns nil |
| `win-cumsum [col]` | Cumulative sum | nil treated as 0 |
| `win-cummin [col]` | Cumulative minimum | nil skipped |
| `win-cummax [col]` | Cumulative maximum | nil skipped |
| `win-cummean [col]` | Cumulative mean | nil skipped |
| `win-rleid [col]` | Run-length encoding group ID (increments on value change) | N/A |
| `win-delta [col]` | `x[i] - x[i-1]`, nil for first element | nil propagates |
| `win-ratio [col]` | `x[i] / x[i-1]` (float division), nil for first element | nil propagates |
| `win-differ [col]` | Boolean: true where value changes from predecessor, true for first element (q convention) | N/A |
| `win-mavg [col width]` | Moving average over trailing `width` elements | nil skipped |
| `win-msum [col width]` | Moving sum over trailing `width` elements | nil skipped |
| `win-mdev [col width]` | Moving standard deviation over trailing `width` elements | nil skipped |
| `win-mmin [col width]` | Moving minimum over trailing `width` elements | nil skipped |
| `win-mmax [col width]` | Moving maximum over trailing `width` elements | nil skipped |
| `win-ema [col period-or-alpha]` | Exponential moving average (period ≥ 1 → `alpha = 2/(1+period)`, else alpha directly) | nil mid-series carries forward last EMA; leading nils remain nil |
| `win-fills [col]` | Forward-fill nulls with last non-null value | Leading nulls remain nil |
| `win-scan [op-kw col]` | Generalized cumulative scan with `+`, `*`, `max`, `min` | Leading nils remain nil; mid-series nils carry last accumulator |

### Key design decisions

- **Rank functions operate on current row order** — they do NOT re-sort internally. `win/rank` assigns ranks 1, 2, 3... based on position, using column values only for tie detection. The `:within-order` keyword controls the sort before rank is computed. This matches SQL semantics where `RANK() OVER (ORDER BY col)` means the ORDER BY controls sorting and RANK assigns positions.
- **Ranking tie-breaking**: `win-rank` skips ranks after ties (`[10,20,20,30]→[1,2,2,4]`), `win-dense-rank` doesn't skip (`→[1,2,2,3]`), `win-row-number` is always sequential (`→[1,2,3,4]`).
- **`win-rleid`** is invaluable for financial regime detection: identifies consecutive runs of the same value, assigning incrementing group IDs.
- **Adjacent-element ops** (`win-delta`, `win-ratio`, `win-differ`) are thin wrappers around `win-lag`. `win-delta` = `x[i] - x[i-1]`, `win-ratio` = `x[i] / x[i-1]` (float division). `win-differ` returns a boolean change flag (true for first element, matching q's `differ` convention).
- **Rolling window functions** share a private helper `rolling-window-vals` that extracts the non-nil values in each trailing window. Edge behavior: expanding window at start (positions with fewer than `width` items compute over available). Nil values within the window are excluded.
- **`win-ema` parameter dispatch**: second arg ≥ 1 → treated as period, converted via `alpha = 2 / (1 + period)`. Second arg < 1 → treated as alpha directly. Seeded at first non-nil value; nil mid-series carries forward last EMA.
- **`win-fills`** forward-fills nulls with the last non-null value. Leading nulls (before first non-null) remain nil, matching q's `fills` convention.

---

## `src/datajure/row.clj` — Row-wise Function Implementations

Each function takes multiple columns (varargs) and returns a single column of the same length. These operate across columns within a single row — the counterpart to window functions (which operate down a single column within a group).

### Functions

| Function | Description | Nil handling |
|---|---|---|
| `row-sum [& cols]` | Sum across columns per row | nil treated as 0 |
| `row-mean [& cols]` | Mean across columns per row | nil skipped |
| `row-min [& cols]` | Min across columns per row | nil skipped |
| `row-max [& cols]` | Max across columns per row | nil skipped |
| `row-count-nil [& cols]` | Count of nil values per row | N/A |
| `row-any-nil? [& cols]` | Boolean: any nil present per row | N/A |

### Nil conventions

- `row-sum`: nil treated as zero (matching R's `rowSums(na.rm=TRUE)` and Stata's `rowtotal`)
- `row-mean`, `row-min`, `row-max`: skip nil values
- All return nil when every input is nil

### Key design decisions

- **No context restrictions**: unlike `win/*` (which requires `:set` context), `row/*` works in any context (`:set`, `:where`, window mode).
- **AST pattern mirrors `win/*`**: `:row` node type, `row-sym->op` lookup, `row-op-table` dispatch — same architecture, different semantics.
- **`col-refs`/`win-refs` traverse `:row` args**: column validation and window detection work through row expressions.

---

## `src/datajure/util.clj` — Data Cleaning Utilities

Standalone functions that operate on datasets directly and thread naturally with `dt`. Not part of the `dt` query function — these complement it for common data preparation tasks.

### Functions

| Function | Description |
|---|---|
| `describe [dataset]` / `describe [dataset cols]` | Summary stats (n, mean, sd, min, p25, median, p75, max, n-missing). Returns a dataset. Non-numeric columns show nil for stats. |
| `clean-column-names [dataset]` | Lowercase, replace spaces/special chars with hyphens. `"Some Ugly Name!"` → `:some-ugly-name` |
| `duplicate-rows [dataset]` / `duplicate-rows [dataset cols]` | Returns dataset of duplicate rows only. Optional column subset. |
| `mark-duplicates [dataset]` / `mark-duplicates [dataset cols]` | Adds `:duplicate?` boolean column. Optional column subset. |
| `drop-constant-columns [dataset]` | Remove columns where all values are identical (zero variance). |
| `coerce-columns [dataset col-type-map]` | Bulk type coercion. `{:year :int64 :mass :float64}` |

### Usage examples

```clojure
(require '[datajure.util :as du])

(du/describe ds)                                ;; all columns
(du/describe ds [:mass :height])                ;; subset

(-> (dio/read "messy.csv")
    (du/clean-column-names)
    (du/mark-duplicates [:id :date])
    (core/dt :where #dt/e (not :duplicate?)))

(du/drop-constant-columns ds)
(du/coerce-columns ds {:year :float64 :mass :float64})
```

---

## `src/datajure/io.clj` — Unified File I/O

Extension-dispatching read/write wrapper over `tech.v3.dataset` I/O. Columns are returned as keywords by default.

### Public API

| Function | Description |
|---|---|
| `read [path]` / `read [path opts]` | Read dataset from file. Dispatches on extension. |
| `write [ds path]` / `write [ds path opts]` | Write dataset to file. Dispatches on extension. |
| `read-seq [path]` / `read-seq [path opts]` | Read file as lazy seq of datasets (Parquet only). |

### Supported formats

| Extension | Requires | Notes |
|---|---|---|
| `.csv` `.tsv` | Native | Also `.csv.gz` / `.tsv.gz` |
| `.nippy` | Native | Tech.ml binary format |
| `.parquet` | `tech.v3.libs.parquet` | Throws `:missing-dep` if absent |
| `.xlsx` | `tech.v3.libs.fastexcel` or `tech.v3.libs.poi` | Throws `:missing-dep` if absent |
| `.xls` | `tech.v3.libs.poi` | Throws `:missing-dep` if absent |
| `.arrow` `.feather` | `tech.v3.libs.arrow` | Throws `:missing-dep` if absent |

### Extension dispatch

Extension is extracted case-insensitively, stripping `.gz` suffix first: `"data.CSV.GZ"` → `:csv`.

### Usage

```clojure
(require '[datajure.io :as dio])

(def ds (dio/read "data.csv"))
(def ds (dio/read "data.parquet"))          ;; needs tech.v3.libs.parquet dep
(def ds (dio/read "data.tsv.gz"))
(def ds (dio/read "data.csv" {:separator \tab}))

(dio/write ds "output.csv")
(dio/write ds "output.parquet")
(dio/write ds "output.tsv.gz")

;; Streaming read for large Parquet files
(doseq [chunk (dio/read-seq "huge.parquet")]
  (process chunk))
```

### Key notes

- `(:refer-clojure :exclude [read])` in ns — `dio/read` intentionally shadows `clojure.core/read`.
- Options pass through to the underlying `tech.v3.dataset.io` functions unchanged (except `:key-fn keyword` is merged as default).

---

## `src/datajure/reshape.clj` — Reshaping

### Public API

| Function | Description |
|---|---|
| `melt [dataset opts]` | Wide→long reshape. |

### `melt` options map

| Key | Required | Default | Description |
|---|---|---|---|
| `:id` | Yes | — | Vector of column keywords to keep as identifiers |
| `:measure` | No | all non-id cols | Vector of column keywords to stack |
| `:variable-col` | No | `:variable` | Keyword for the new variable column |
| `:value-col` | No | `:value` | Keyword for the new value column |

### Usage

```clojure
(require '[datajure.reshape :refer [melt]])

;; Basic melt
(melt ds {:id [:species :year] :measure [:mass :flipper :bill]})

;; Infer measure cols (all non-id)
(melt ds {:id [:species :year]})

;; Custom output column names
(melt ds {:id [:species :year] :measure [:mass :flipper]
          :variable-col :metric :value-col :val})

;; Thread with dt
(-> ds
    (melt {:id [:species :year] :measure [:mass :flipper]})
    (core/dt :by [:species :variable] :agg {:avg #dt/e (mn :value)}))
```

### Implementation note

`ds/pivot->longer` is not available in `tech.ml.dataset` 8.003. `melt` is implemented manually: for each measure column, select id + measure, rename measure → value-col, add a constant `:variable` column with the measure column name as a string, then `ds/concat` all slices.

---

## `src/datajure/join.clj` — Joins

Standalone `join` function wrapping `tech.v3.dataset.join/pd-merge` with keyword-driven syntax matching the datajure spec. Returns a dataset for piping with `dt`.

### Public API

| Function | Description |
|---|---|
| `join [left right & opts]` | Join two datasets with `:on`, `:how`, `:left-on`/`:right-on`, `:validate`, `:report` |

### Options

| Key | Required | Default | Description |
|---|---|---|---|
| `:on` | Yes* | — | Column keyword or vector of keywords (same name in both datasets) |
| `:left-on` | Yes* | — | Column keyword(s) for left dataset (use with `:right-on`) |
| `:right-on` | Yes* | — | Column keyword(s) for right dataset (use with `:left-on`) |
| `:how` | No | `:inner` | Join type: `:inner`, `:left`, `:right`, `:outer` |
| `:validate` | No | — | Cardinality check: `:1:1`, `:1:m`, `:m:1`, `:m:m` |
| `:report` | No | `false` | Print merge diagnostics (matched/left-only/right-only counts) |

*Must provide either `:on` or both `:left-on` and `:right-on`.

### `:validate` cardinality checks

Runs **before** the join executes — checks key uniqueness on the source datasets to prevent silent data multiplication:

| Value | Left keys | Right keys |
|---|---|---|
| `:1:1` | Must be unique | Must be unique |
| `:1:m` | Must be unique | Can duplicate |
| `:m:1` | Can duplicate | Must be unique |
| `:m:m` | No restriction | No restriction |

### `:report` merge diagnostics

Prints a one-line summary based on unique key set operations:
```
[datajure] join report: 2 matched, 1 left-only, 1 right-only
```

### Usage

```clojure
(require '[datajure.join :refer [join]])

;; Basic joins
(join X Y :on :id :how :left)
(join X Y :on [:a :b] :how :inner)

;; Asymmetric key names
(join X Y :left-on :id :right-on :key :how :left)

;; With cardinality validation
(join X Y :on :id :how :left :validate :m:1)

;; With merge diagnostics
(join X Y :on :id :how :left :report true)

;; Pipeline with dt
(-> (join X Y :on :id :how :left :validate :m:1 :report true)
    (core/dt :where #dt/e (> :year 2008)
             :agg {:total #dt/e (sm :mass)}))
```

### Error handling

| `:dt/error` | Condition |
|---|---|
| `:join-invalid-keys` | `:on` combined with `:left-on`/`:right-on` |
| `:join-missing-keys` | Neither `:on` nor both `:left-on`/`:right-on` provided |
| `:join-unknown-how` | Unrecognized `:how` value |
| `:join-unknown-validate` | Unrecognized `:validate` value |
| `:join-cardinality-violation` | Key uniqueness check failed (includes `:dt/side`, `:dt/keys`, `:dt/validate` in ex-data) |

---

## `src/datajure/core.clj` — `dt` Query Function

### Public API

| Symbol | Type | Description |
|---|---|---|
| `dt` | fn | Main query function |
| `N` | var | Row count helper (`ds/row-count`) |
| `mean` | var | Column mean (alias for `dfn/mean`) |
| `sum` | var | Column sum (alias for `dfn/sum`) |
| `median` | var | Column median (alias for `dfn/median`) |
| `stddev` | var | Column standard deviation (alias for `dfn/standard-deviation`) |
| `variance` | var | Column variance (alias for `dfn/variance`) |
| `max*` | var | Column maximum (alias for `dfn/reduce-max`) |
| `min*` | var | Column minimum (alias for `dfn/reduce-min`) |
| `count* [col]` | fn | Count of non-nil values in column |
| `asc [col]` | fn | Sort spec `{:order :asc :col col}` |
| `desc [col]` | fn | Sort spec `{:order :desc :col col}` |
| `pass-nil [f & guard-cols]` | fn | Wraps plain row fn to return nil if any guard column is nil/missing |
| `rename [dataset col-map]` | fn | Rename columns without dropping any |
| `xbar [col-kw width]` / `xbar [col-kw width unit]` | fn | Floor-division bucketing for computed `:by`. Numeric: floors to nearest multiple of width. Temporal: floors to nearest width-unit boundary (`:seconds`, `:minutes`, `:hours`, `:days`, `:weeks`). Returns nil for nil input. |
| `*dt*` | dynamic var | Last dataset result in REPL session (bound by `datajure.nrepl/wrap-dt` middleware) |
| `reset-notes! []` | fn | Reset shown one-time info notes (useful for testing) |

### `dt` function signature

```clojure
(dt dataset
  :where        <predicate>
  :set          <derivations>
  :agg          <aggregations>
  :by           <grouping>
  :within-order <sort-spec>
  :select       <column-selector>
  :order-by     <sort-specs>)
```

All keywords optional. Fixed evaluation order regardless of keyword order in call:
```
:where → :set (or :agg) → :select → :order-by
```

### `:by` × `:set`/`:agg` Matrix

|            | No `:by`                                       | With `:by`                        |
|------------|------------------------------------------------|-----------------------------------|
| **`:set`** | Column derivation (+ whole-dataset window mode if `win/*` present) | Partitioned window mode (all rows preserved) |
| **`:agg`** | Whole-table summary                            | Group aggregation (rows collapsed)|

When `:set` contains `win/*` functions without `:by`, the entire dataset is treated as a single partition — matching SQL's `OVER()` without `PARTITION BY`.

### `:by` accepts two forms

```clojure
;; Vector of keywords (standard)
(dt ds :by [:species] :agg {:n N})

;; Plain fn of row → map (computed grouping)
(dt ds :by (fn [row] {:heavy? (> (:mass row) 4000)}) :agg {:n N})
```

### `:within-order`

- Valid with `:set` (with or without `:by`). Throws `:within-order-invalid` with `:agg` or without `:set`.
- With `:by`: sorts rows within each partition before derivations are applied.
- Without `:by`: sorts the entire dataset before derivations are applied.
- Uses same `asc`/`desc` grammar as `:order-by`.
- When omitted, window functions operate on current row order.

```clojure
;; Partitioned window with ordering
(dt ds :by [:species] :within-order [(desc :mass)] :set {:rank #dt/e (win/rank :mass)})

;; Whole-dataset window with ordering
(dt ds :within-order [(asc :date)] :set {:cumret #dt/e (win/cumsum :ret)})
```

### `:select` polymorphic dispatch

```clojure
(dt ds :select [:species :mass])           ;; vector of keywords
(dt ds :select :species)                   ;; single keyword
(dt ds :select [:not :year :height])       ;; exclusion
(dt ds :select :type/numerical)            ;; all numeric columns
(dt ds :select :!type/numerical)           ;; all non-numeric columns
(dt ds :select #"mass.*")                  ;; regex on column names
(dt ds :select #(not= % :year))            ;; predicate fn on column keyword
(dt ds :select {:species :sp :mass :m})    ;; map = select + rename
```

Type selectors use `tech.v3.datatype.casting/numeric-type?` on column `:datatype` metadata.

### Column validation

`validate-expr-cols` runs before any `#dt/e` expression executes:
- Extracts column refs via `expr/col-refs` (including through composed expressions)
- Throws `:dt/error :unknown-column` with structured `ex-data`
- Includes **Levenshtein suggestions** for typos (edit distance ≤ 3)

```clojure
(dt ds :where #dt/e (> :maas 4000))
;; => ExceptionInfo: Unknown column(s) #{:maas} in :where expression
;;    ex-data: {:dt/error :unknown-column
;;              :dt/columns #{:maas}
;;              :dt/closest {:maas [:mass]}
;;              :dt/available [:mass :species :year]}
```

### Window context validation

- `win/*` in `:where` → error `:win-outside-window` (always)
- `win/*` in `:agg` → error `:win-outside-window` (always)
- `win/*` in `:set` without `:by` → **allowed** (whole-dataset window mode, via `apply-window-set`)
- `win/*` in `:set` with `:by` → **allowed** (partitioned window mode, via `apply-group-set`)

The `derivations-have-win?` helper detects `win/*` references in `:set` derivations to route to the correct execution path.

### Map `:set` cross-reference validation (phase 21)

`validate-map-set-cross-refs` runs on map-form `:set` before execution (in all modes: plain `:set`, window mode with `:by`, and window mode without `:by`). If any `#dt/e` expression references a column being derived in the same map, it throws `:map-set-cross-reference` with structured `ex-data`:

```clojure
(dt ds :set {:mass-k  #dt/e (/ :mass 1000)
             :mass-2k #dt/e (* :mass-k 2)})
;; => ExceptionInfo: In map-form :set, column :mass-2k references #{:mass-k},
;;    which are being derived in the same map. Map semantics are simultaneous —
;;    use vector-of-pairs [[:col1 expr1] [:col2 expr2]] for sequential derivation.
```

### One-time REPL info notes (phase 22+29)

Informational notes printed once per REPL session via `shown-notes` atom:

| Key | Trigger | Message |
|---|---|---|
| `:agg-no-by` | `:agg` without `:by` | "Aggregating over entire dataset. Use :by for group aggregation." |
| `:window-mode` | `:by` + `:set` (first use) | "Window mode: computing within groups, keeping all rows." |
| `:window-mode-no-by` | `:set` with `win/*` but no `:by` | "Window mode (whole dataset): computing over entire dataset, keeping all rows." |
| `:window-no-order` | window mode without `:within-order` | "Window mode using current row order. Use :within-order to sort within groups." |

`reset-notes!` resets the atom — call it in tests that assert on note behavior.

### Disallowed combinations

| Combination | Error key | Message |
|---|---|---|
| `:set` + `:agg` in same call | `:set-agg-conflict` | "Cannot combine :set and :agg. Use -> threading." |
| `:within-order` with `:agg` | `:within-order-invalid` | ":within-order is not valid with :agg." |
| `:within-order` without `:set` | `:within-order-invalid` | ":within-order requires :set." |
| `win/*` in `:where` | `:win-outside-window` | "Window function(s) require :set context." |
| `win/*` in `:agg` | `:win-outside-window` | "Window function(s) require :set context." |

### Internal dispatch predicate

```clojure
(defn- expr-node? [x]
  (and (map? x) (contains? x :node/type)))
```

Used throughout to dispatch between `#dt/e` AST and plain fn.

### Evaluation order in `dt`

```
1. :where
2a. :by + :set             →  apply-group-set    (partitioned window mode, respects :within-order)
2b. :set + win/* (no :by)  →  apply-window-set   (whole-dataset window mode, respects :within-order)
2c. :set (no win/*)        →  apply-set           (column derivation)
2d. :by + :agg             →  apply-group-agg     (group aggregation)
2e. :agg                   →  apply-agg           (whole-table summary)
3. :select
4. :order-by
```

### `apply-window-set` (phase 29)

Handles `win/*` in `:set` without `:by`. Sorts by `:within-order` if present, then delegates to `apply-set`. The entire dataset is the single partition.

### Important: `(declare apply-order-by)` at top of file

`apply-group-set` and `apply-window-set` call `apply-order-by` for `:within-order` support, but `apply-order-by` is defined later in the file. A forward declaration is required to avoid load-order compilation errors.

---

## `src/datajure/nrepl.clj` — REPL `*dt*` Auto-binding

nREPL middleware that automatically binds `datajure.core/*dt*` to the last dataset result, mirroring how Clojure's `*1` works but only for `tech.v3.dataset` results.

### Installation

```clojure
;; .nrepl.edn (recommended)
{:middleware [datajure.nrepl/wrap-dt]}

;; Or in deps.edn :nrepl alias
{:main-opts ["-m" "nrepl.cmdline"
             "--middleware" "[datajure.nrepl/wrap-dt]"]}
```

### How it works

1. **`wrap-dt`** middleware initializes `*dt*` in the nREPL session (if absent) and wraps the transport for `"eval"` ops only.
2. **`wrapping-transport`** intercepts `:value` responses. On the eval thread, it checks `*1` — if it's a `tech.v3.dataset.impl.dataset.Dataset`, it `set!`s `core/*dt*` to that value.
3. Non-eval ops (describe, clone, etc.) pass through unwrapped.
4. `dt` itself has **no side effects** — it never writes to `*dt*`. The middleware handles binding, same pattern as `*1`.

### Usage

```clojure
user=> (core/dt ds :by [:species] :agg {:n core/N})
;; => dataset result...

user=> (core/dt core/*dt* :order-by [(core/desc :n)])
;; *dt* holds the previous dataset result
```

### Key design decisions

- **`*dt*` is `^:dynamic`** — bound per-session via nREPL's thread-binding mechanism, not global state.
- **`dataset?` uses `instance?`** on `tech.v3.dataset.impl.dataset.Dataset` for reliable type checking.
- **`set-descriptor!`** registers the middleware with nREPL's dependency system (requires `"session"`, expects `"eval"`).

---

## `src/datajure/clerk.clj` — Rich Clerk Notebook Viewers

Optional namespace providing custom Clerk viewers for Datajure types. Requires Clerk on the classpath (`:clerk` alias).

### Public API

| Function / Var | Description |
|---|---|
| `install! []` | Register all Datajure viewers with Clerk via `clerk/add-viewers!` |
| `dataset->hiccup [ds]` / `dataset->hiccup [ds opts]` | Dataset → rich Hiccup table with type badges, nil indicators, truncation |
| `expr->hiccup [node]` | `#dt/e` AST → Hiccup with syntax display, column refs, window badges |
| `describe->hiccup [desc-ds]` | `du/describe` result → Hiccup with missing-data highlighting |
| `ast->string [node]` | `#dt/e` AST → readable string (reconstructs `cond` from nested `if`, handles composition) |
| `dataset-viewer` | Viewer map `{:pred dataset? :transform-fn ...}` |
| `expr-viewer` | Viewer map `{:pred expr-node? :transform-fn ...}` |
| `describe-viewer` | Viewer map `{:pred ... :transform-fn ...}` (matches datasets with `:column`, `:n`, `:mean` cols) |

### Viewer features

**Dataset viewer** — gradient header bar with dataset name/dimensions, column name headers with type badges (color-coded: green=numeric, blue=string, orange=boolean), per-column nil counts in red, alternating row stripes, right-aligned monospace numbers, nil values in grey italic, row/column truncation with indicators.

**Expression viewer** — pink/red gradient header with "#dt/e Expression" label, dark-background monospace code display, metadata footer showing referenced columns and window ops, "window" badge for expressions containing `win/*`.

**Describe viewer** — green gradient header, right-aligned monospace stats, missing counts highlighted in red bold, nil stats shown as "—".

**`ast->string`** — reconstructs `cond` from nested `:if` chains (detects `true` pred as `:else`), handles expression composition (recursively renders `:lit` nodes containing expr-nodes), displays user-friendly op names (`mean` not `:mn`). Handles all AST node types including `:scan` (`win/scan`) and `:xbar` (with optional temporal unit).

### Usage

```clojure
;; In a Clerk notebook:
(ns my-notebook
  (:require [datajure.clerk :as dc]
            [datajure.core :as core]
            [nextjournal.clerk :as clerk]))
(dc/install!)

;; All datasets now render as rich tables automatically
(core/dt ds :by [:species] :agg {:n core/N :avg #dt/e (mn :mass)})

;; #dt/e AST nodes render with syntax highlighting
#dt/e (/ :mass (sq :height))

;; du/describe gets enhanced formatting
(du/describe ds)
```

### Key design decisions

- **`requiring-resolve` for Clerk dependency** — `datajure.clerk` can be loaded without Clerk on the classpath (the `transform-fn` fns use `requiring-resolve` at call time). But `install!` will fail if Clerk isn't present.
- **Describe viewer registered first** — `add-viewers!` prepends, and Clerk searches linearly. Describe viewer's more specific pred fires before the general dataset viewer for `du/describe` output.
- **Hiccup generation is public** — `dataset->hiccup`, `expr->hiccup`, `describe->hiccup`, and `ast->string` are all public for testing and reuse (e.g., in custom viewers or static HTML export).

### Sample notebook

`notebooks/datajure_demo.clj` — demonstrates all three viewers with filter, derive, group+aggregate, window functions, conditional derivation, nil handling, expression display, and describe output.

Start with: `(clerk/serve! {:watch-paths ["notebooks"]})`

---

## `src/datajure/clay.clj` — Clay/Kindly Notebook Integration

Optional namespace providing Clay/Kindly-compatible rendering for Datajure types. Uses the Kindly convention — values are annotated with `:kind/hiccup` metadata so any Kindly-compatible tool (Clay, Portal, etc.) renders them as rich HTML.

### Public API

| Function | Description |
|---|---|
| `view [dataset]` / `view [dataset opts]` | Dataset → Kindly-annotated rich Hiccup table |
| `view-expr [node]` | `#dt/e` AST → Kindly-annotated expression display |
| `view-describe [desc-ds]` | `du/describe` result → Kindly-annotated enhanced table |
| `datajure-advisor [context]` | Kindly advisor fn: returns `[[:kind/hiccup]]` for Datajure types |
| `install! []` | Register custom Kindly advisor for auto-rendering in Clay notebooks |

### Two usage modes

**1. Explicit wrapping** (no install needed, works anywhere):

```clojure
(require '[datajure.clay :as dc])

(dc/view ds)                              ;; rich dataset table
(dc/view ds {:max-rows 10})               ;; with options
(dc/view-expr #dt/e (/ :mass 1000))       ;; expression display
(dc/view-describe (du/describe ds))        ;; enhanced describe
```

**2. Auto-rendering via install!** (registers Kindly advisor):

```clojure
(ns my-notebook
  (:require [datajure.clay :as dc]
            [datajure.core :as core]
            [scicloj.clay.v2.api :as clay]))
(dc/install!)

;; All datasets and #dt/e exprs now auto-render in Clay
(core/dt ds :by [:species] :agg {:n core/N})
```

### How it works

Unlike Clerk (which uses `add-viewers!` to register viewer maps), Clay uses the **Kindly advisor** system. `install!` calls `kindly-advice/set-advisors!` to prepend a custom advisor that:

1. Detects Datajure types (describe output → dataset → `#dt/e` expr, in priority order)
2. Transforms the value to its Hiccup representation via `datajure.clerk`'s public Hiccup generators
3. Sets `:kind :kind/hiccup` on the Kindly context so Clay renders it as HTML

The explicit `view`/`view-expr`/`view-describe` functions simply call the Hiccup generators and attach `:kindly/kind :kind/hiccup` metadata via `vary-meta`.

### Key design decisions

- **Reuses `datajure.clerk` Hiccup generators** — `dataset->hiccup`, `expr->hiccup`, `describe->hiccup` are the shared rendering core. No code duplication between Clerk and Clay integrations.
- **No Clay dependency at load time** — `datajure.clay` only requires `datajure.clerk` and `tech.v3.dataset`. Clay/Kindly are resolved via `requiring-resolve` in `install!`, so the namespace loads without Clay on the classpath.
- **Kindly metadata convention** — explicit `view` functions use `vary-meta` with `:kindly/kind :kind/hiccup`, the standard Kindly annotation. Any Kindly-compatible tool renders them.
- **Describe before dataset** — advisor checks describe output first (more specific predicate), then generic dataset, matching Clerk's registration order.

### Sample notebook

`notebooks/datajure_clay_demo.clj` — demonstrates auto-rendering, filter, derive, group+aggregate, window functions, expression display, composed expressions, conditional derivation, full pipeline, explicit wrapping, and describe output.

Start with:
```clojure
(require '[scicloj.clay.v2.api :as clay])
(clay/make! {:source-path "notebooks/datajure_clay_demo.clj"})
```

---

## `src/datajure/concise.clj` — Short Aliases

Opt-in namespace for power users. Require only what you need.

```clojure
(require '[datajure.concise :refer [mn sm md sd ct nuniq fst lst wa ws N dt asc desc rename pass-nil]])
```

| Symbol | Full name | Source |
|---|---|---|
| `mn` | `core/mean` | `datajure.core` |
| `sm` | `core/sum` | `datajure.core` |
| `md` | `core/median` | `datajure.core` |
| `sd` | `core/stddev` | `datajure.core` |
| `mx` | `core/max*` | `datajure.core` |
| `mi` | `core/min*` | `datajure.core` |
| `ct` | `dtype/ecount` | `tech.v3.datatype` |
| `nuniq` | `(fn [col] (count (distinct (dtype/->reader col))))` | `datajure.expr` |
| `fst` | first value in column (`first-val`) | `datajure.expr` |
| `lst` | last value in column (`last-val`) | `datajure.expr` |
| `wa` | weighted average (`wavg`) | `datajure.expr` |
| `ws` | weighted sum (`wsum`) | `datajure.expr` |
| `N` | `core/N` | `datajure.core` |
| `dt` | `core/dt` | `datajure.core` |
| `asc` | `core/asc` | `datajure.core` |
| `desc` | `core/desc` | `datajure.core` |
| `rename` | `core/rename` | `datajure.core` |
| `pass-nil` | `core/pass-nil` | `datajure.core` |

---

## `#dt/e` Reader Tag

### Registration
`resources/data_readers.clj` → `{dt/e datajure.expr/read-expr}` loaded at JVM startup.

---

## Error Handling

All errors use `ex-info` with `:dt/error` keyword in `ex-data`:

| `:dt/error` | Condition |
|---|---|
| `:unknown-column` | `#dt/e` references column not in dataset, or `:select` references unknown column (includes Levenshtein suggestions; traverses composed expressions) |
| `:set-agg-conflict` | `:set` and `:agg` in same `dt` call |
| `:within-order-invalid` | `:within-order` used with `:agg` or without `:set` |
| `:win-outside-window` | `win/*` used outside `:set` context (i.e. in `:where` or `:agg`) |
| `:unknown-win-op` | Unrecognized window op keyword in AST |
| `:unknown-row-op` | Unrecognized row op keyword in AST |
| `:map-set-cross-reference` | Map-form `:set` expression references a sibling column being derived in the same map |
| `:unsupported-format` | `dio/read` or `dio/write` called with unrecognized file extension |
| `:missing-dep` | Optional format (parquet, xlsx, arrow) used without required library on classpath |
| `:join-invalid-keys` | `join` called with both `:on` and `:left-on`/`:right-on` |
| `:join-missing-keys` | `join` called without `:on` or both `:left-on`/`:right-on` |
| `:join-unknown-how` | Unrecognized `:how` value in `join` |
| `:join-unknown-validate` | Unrecognized `:validate` value in `join` |
| `:join-cardinality-violation` | Key uniqueness check failed in `join` (includes `:dt/side`, `:dt/keys`, `:dt/validate`) |

---

## Tests

Run via REPL (`:nrepl` alias puts `test/` on classpath):

```clojure
(load-file "test/datajure/core_test.clj")
(load-file "test/datajure/concise_test.clj")
(load-file "test/datajure/util_test.clj")
(load-file "test/datajure/io_test.clj")
(load-file "test/datajure/reshape_test.clj")
(load-file "test/datajure/join_test.clj")
(load-file "test/datajure/nrepl_test.clj")
(load-file "test/datajure/clerk_test.clj")
(load-file "test/datajure/clay_test.clj")
(clojure.test/run-tests 'datajure.core-test 'datajure.concise-test 'datajure.util-test
                        'datajure.io-test 'datajure.reshape-test 'datajure.join-test
                        'datajure.nrepl-test 'datajure.clerk-test 'datajure.clay-test)
```

### Test coverage (209 tests, 693 assertions)

| File | Tests | Assertions |
|---|---|---|
| `core_test.clj` | 98 | 450 |
| `concise_test.clj` | 5 | 18 |
| `util_test.clj` | 15 | 37 |
| `io_test.clj` | 10 | 16 |
| `reshape_test.clj` | 8 | 13 |
| `join_test.clj` | 31 | 55 |
| `nrepl_test.clj` | 10 | 22 |
| `clerk_test.clj` | 19 | 49 |
| `clay_test.clj` | 13 | 33 |

Core test categories: boolean mask regression, `:in`/`:between?`, `:by+:agg`, `:by+:set` window mode, `:within-order`, `:order-by`, `:select` (all forms incl. type selectors), plain fn paths, `:set` semantics (simultaneous/sequential), whole-table agg, threading, column validation (incl. Levenshtein), `if`/`cond`/`let`/`coalesce` special forms, `count-distinct`, `pass-nil`, `rename`, window AST parsing, window col-refs, window context validation, window runtime (rank/dense-rank/row-number, lag/lead, cumsum/cummin/cummax/cummean, rleid, composite expressions, within-order interaction), **whole-dataset window mode** (cumsum/lag/rank without :by, :within-order without :by, composite win expressions without :by, validation of :within-order without :set and with :agg), **expression composition** (basic reuse in :set/:where/:agg, nested composition with aggregation/comparison/stddev, col-refs traversal through composed exprs, column validation on composed exprs), **row-wise functions** (AST parsing for all 6 row/* symbols, col-refs/win-refs traversal, row/sum nil-as-zero, row/mean skip-nil, row/min+max, row/count-nil+any-nil?, column validation, window mode compatibility), **adjacent-element ops** (win/delta basic+nil, win/ratio basic+nil+composite `(- (win/ratio :price) 1)`, win/differ basic+repeated-values, per-partition behavior, :within-order interaction), **rolling window functions** (win/mavg+msum+mdev+mmin+mmax basic output, nil skipping, all-nil window, width=1, per-partition independence, :within-order interaction, composite expressions), **EMA + forward-fill** (win/ema period-based+alpha-based+nil handling, win/fills basic+leading-nils-remain+per-partition+:within-order interaction), **win/scan** (cumulative scan +/*/max/min, nil handling, per-partition with :by, wealth-index composite, :within-order interaction, col-refs/win-refs traversal, unknown-op error), **util functions** (describe all-columns/subset/missing, clean-column-names basic/special-chars, duplicate-rows all-columns/subset/no-duplicates, mark-duplicates basic/subset, drop-constant-columns basic/all-varying, coerce-columns basic/partial), **join functions** (inner/left/right/outer, default :how, multi-column :on, left-on/right-on, :validate :1:1/:1:m/:m:1/:m:m pass+fail, :validate unknown value, :validate with left-on/right-on, :validate with multi-column :on, :report basic/full-overlap/no-output-by-default/with-left-on-right-on, :validate+:report together, ex-data structure, no-overlap join, composable-with-dt, full pipeline), **nrepl middleware** (*dt* dynamic var, dataset? detection, wrapping-transport dataset binding, non-dataset passthrough, non-value passthrough, response forwarding, session initialization, eval-only wrapping, sequential updates, middleware descriptor), **clerk viewers** (dataset->hiccup basic/truncation/col-truncation/nil-values/type-badges, ast->string basic-ops/window-ops/row-ops/if-and-cond/let/coalesce/membership/composition, expr->hiccup basic/window-badge, describe->hiccup basic/missing-highlight, viewer predicates), **clay integration** (view basic/with-opts/nil-values, view-expr basic/window/composition, view-describe basic/missing-highlight, datajure-advisor dataset/expr/describe/other/priority-order — all verify Kindly :kind/hiccup metadata annotation)., **xbar** (AST parsing, col-refs, numeric bucketing, `:by` usage, nil handling, temporal units, standalone fn, unknown unit error), **within-order-invalid** error paths (`:within-order` with `:agg`, `:within-order` without `:set`).

---

## Implementation Status Summary

Phases 1–41 are all implemented and tested. All previously identified bugs and code review items have been resolved.

**Documentation completeness:** All public vars across all 12 namespaces have docstrings.

---

## Remaining Spec Features

### Near-term (v2.1 scope — not yet implemented)

These are the next actionable items identified by comparing the spec against the current implementation:

| Item | Notes |
|---|---|
| `stat/*` namespace (`stat/standardize`, `stat/demean`, `stat/winsorize`) | Column-level statistical transforms. |
| `between` column selector (positional range) | E.g. `(dt ds :select (between :month-01 :month-12))`. |
| As-of join (`:how :asof`, q's `aj`) | Syntax design settled in spec; needs sorted-merge implementation. |
| `grp`, `gi` aliases in `datajure.concise` | See note below — deferred pending design decisions. |

### Deferred: `grp` and `gi`

**`grp`** (data.table's `.SD` — current group as a dataset): In Datajure's design, plain functions in `:agg` already receive the group dataset as `%`, so `grp` as `identity` would only be useful if there were a mechanism to apply a function across all columns simultaneously (like `.SD` + `lapply`). No such mechanism exists in the current design, and the plain-function `:agg` path already covers the use case. **`grp` is not worth implementing** in its current form — it has no actionable semantics beyond what `%` already provides.

**`gi`** (data.table's `.GRP`/`.I` — group index or per-group row indices): Neither the group number nor original row indices are currently tracked internally. The `apply-group-agg` and `apply-group-set` functions receive group sub-datasets from `ds/group-by` without index metadata. Implementing `gi` would require: (1) choosing semantics (scalar group number vs. row index vector), (2) enumerating groups with their indices in the dispatch loop, (3) a mechanism to expose the value inside expressions (`#dt/e` only sees dataset columns; plain fns receive the sub-dataset). **Deferred** — non-trivial plumbing with unclear ergonomics. Note: `win/row-number` without `:by` already gives sequential row numbers globally; `:by` + `win/row-number` gives per-group row numbers within `:set` context, which covers most practical use cases.

### Open design question (unresolved from spec)

**`:within-order` + `:agg`**: Should `:within-order` be allowed with `:agg` to support `first-val`/`last-val` order-dependent aggregation? Currently throws `:within-order-invalid`. The spec explicitly left this open — "decide during v2.0 implementation based on how frequently `first-val`/`last-val` appear in real-world `:agg` queries." Currently users work around this by pre-sorting via threading before the `:agg` call.

### Long-term (v2.1+/v3 scope — deferred)

- Window join (`:how :window`, q's `wj`) — v2.1/v3
- Per-expression windowing (`over`) — v3
- `under` transform-operate-untransform (BQN Under `⌾`) — v3
- Lazy pipeline optimization (predicate/projection pushdown) — v3
- Backend-agnostic AST compilation (clojask, Spark, DuckDB) — v3

---

## Complete Usage Examples

```clojure
(require '[datajure.core :as core])

;; Filter
(core/dt ds :where #dt/e (> :mass 4000))
(core/dt ds :where #dt/e (and (> :mass 4000) (not (= :species "Adelie"))))
(core/dt ds :where #dt/e (in :species #{"Gentoo" "Chinstrap"}))
(core/dt ds :where #dt/e (between? :year 2007 2009))

;; Select
(core/dt ds :select [:species :mass])
(core/dt ds :select :type/numerical)
(core/dt ds :select :!type/numerical)
(core/dt ds :select [:not :year])
(core/dt ds :select #"body-.*")
(core/dt ds :select {:species :sp :mass :m})   ;; rename-on-select

;; Derive columns
(core/dt ds :set {:bmi #dt/e (/ :mass (sq :height))})
(core/dt ds :set [[:bmi #dt/e (/ :mass (sq :height))]
                  [:obese #dt/e (> :bmi 30)]])  ;; sequential

;; Conditional derivation
(core/dt ds :set {:size #dt/e (cond (> :mass 5000) "large"
                                    (> :mass 3500) "medium"
                                    :else          "small")})
(core/dt ds :set {:adj #dt/e (let [bmi  (/ :mass (sq :height))
                                   base (if (> :year 2010) 1.1 1.0)]
                               (* base bmi))})

;; Nil handling
(core/dt ds :set {:mass #dt/e (coalesce :mass 0.0)})
(core/dt ds :set {:x (core/pass-nil #(Integer/parseInt (:x-str %)) :x-str)})

;; Aggregation
(core/dt ds :agg {:n core/N :avg #dt/e (mn :mass)})
(core/dt ds :by [:species] :agg {:n core/N :avg #dt/e (mn :mass)})
(core/dt ds :by [:year] :agg {:n-species #dt/e (count-distinct :species)})

;; Computed :by
(core/dt ds :by (fn [row] {:heavy? (> (:mass row) 4000)}) :agg {:n core/N})

;; Window mode — partitioned (with :by)
(core/dt ds :by [:species] :set {:mean-mass #dt/e (mn :mass)})

;; Window mode — partitioned with win/* functions
(core/dt ds :by [:species]
    :within-order [(core/desc :mass)]
    :set {:rank   #dt/e (win/rank :mass)
          :cumul  #dt/e (win/cumsum :mass)
          :prev   #dt/e (win/lag :mass 1)})

;; Window mode — whole dataset (without :by)
(core/dt ds
    :within-order [(core/asc :date)]
    :set {:cumret #dt/e (win/cumsum :ret)
          :prev   #dt/e (win/lag :price 1)})

;; Window — composite expression (win/lag inside arithmetic)
(core/dt ds :by [:species]
    :within-order [(core/asc :year)]
    :set {:change #dt/e (- :mass (win/lag :mass 1))})

;; Window — multi-lag construction (sequential :set)
(core/dt ds :by [:permno]
    :within-order [(core/asc :date)]
    :set [[:ret-1  #dt/e (win/lag :ret 1)]
          [:ret-2  #dt/e (win/lag :ret 2)]
          [:ret-12 #dt/e (win/lag :ret 12)]])

;; Window — rleid for regime detection
(core/dt ds :by [:permno]
    :within-order [(core/asc :date)]
    :set {:regime #dt/e (win/rleid :sign-ret)})

;; Adjacent-element ops (inspired by q deltas/ratios)
(core/dt ds :by [:permno]
    :within-order [(core/asc :date)]
    :set {:ret       #dt/e (- (win/ratio :price) 1)
          :price-chg #dt/e (win/delta :price)
          :changed   #dt/e (win/differ :signal)})

;; Rolling window functions
(core/dt ds :by [:permno]
    :within-order [(core/asc :date)]
    :set {:ma-20   #dt/e (win/mavg :price 20)
          :vol-20  #dt/e (win/mdev :ret 20)
          :msum-5  #dt/e (win/msum :volume 5)
          :lo-52w  #dt/e (win/mmin :price 252)
          :hi-52w  #dt/e (win/mmax :price 252)})

;; Exponential moving average
(core/dt ds :by [:permno]
    :within-order [(core/asc :date)]
    :set {:ema-10 #dt/e (win/ema :price 10)})

;; Forward-fill missing values
(core/dt ds :by [:permno]
    :within-order [(core/asc :date)]
    :set {:price #dt/e (win/fills :price)})

;; Reusable expressions — store in vars, compose freely
(def bmi #dt/e (/ :mass (sq :height)))
(def high-mass #dt/e (> :mass 4000))
(core/dt ds :set {:bmi bmi})                                  ;; direct reuse
(core/dt ds :where high-mass)                                  ;; direct reuse
(core/dt ds :by [:species] :agg {:avg-bmi #dt/e (mn bmi)})    ;; composition
(core/dt ds :where #dt/e (> bmi 2.4))                          ;; composition
(core/dt ds :set {:bmi bmi :bmi-high #dt/e (> bmi 2.4)})      ;; mixed

;; Rename
(core/rename ds {:mass :weight-kg :species :penguin-species})

;; Sort
(core/dt ds :order-by [(core/desc :mass) (core/asc :year)])

;; Threading
(-> ds
    (core/dt :where #dt/e (> :year 2007))
    (core/dt :by [:species] :agg {:n core/N :avg #dt/e (mn :mass)})
    (core/dt :order-by [(core/desc :avg)]))

;; Post-agg filter (HAVING)
(-> ds
    (core/dt :by [:species] :agg {:n core/N})
    (core/dt :where #dt/e (>= :n 10)))

;; Top-N per group
(-> ds
    (core/dt :by [:species] :within-order [(core/desc :mass)]
        :set {:rank #dt/e (win/rank :mass)})
    (core/dt :where #dt/e (<= :rank 3)))

;; Concise namespace
(require '[datajure.concise :refer [mn sm N dt desc fst lst wa ws]])
(dt ds :by [:species] :agg {:n N :avg #(mn (:mass %)) :total #(sm (:mass %))})

;; first-val / last-val — OHLC-style
(-> trades
    (core/dt :order-by [(core/asc :time)])
    (core/dt :by [:sym :date]
             :agg {:open  #dt/e (first-val :price)
                   :close #dt/e (last-val :price)}))

;; wavg / wsum — VWAP
(core/dt trades :by [:sym :date]
         :agg {:vwap #dt/e (wavg :size :price)
               :vol  #dt/e (wsum :size :price)})

;; Row-wise operations (row/* inside #dt/e)
(core/dt ds :set {:total    #dt/e (row/sum :q1 :q2 :q3 :q4)
                  :avg-q    #dt/e (row/mean :q1 :q2 :q3 :q4)
                  :n-miss   #dt/e (row/count-nil :q1 :q2 :q3 :q4)
                  :any-miss #dt/e (row/any-nil? :q1 :q2 :q3 :q4)
                  :lo       #dt/e (row/min :q1 :q2 :q3 :q4)
                  :hi       #dt/e (row/max :q1 :q2 :q3 :q4)})

;; Data utilities (datajure.util)
(require '[datajure.util :as du])
(du/describe ds)
(du/describe ds [:mass :height])
(du/clean-column-names messy-ds)
(du/duplicate-rows ds [:id :date])
(du/mark-duplicates ds [:id :date])
(du/drop-constant-columns ds)
(du/coerce-columns ds {:year :float64 :mass :float64})

;; File I/O (datajure.io)
(require '[datajure.io :as dio])
(def ds (dio/read "data.csv"))
(def ds (dio/read "data.tsv.gz"))
(dio/write ds "output.csv")
(dio/write ds "output.parquet")   ;; needs tech.v3.libs.parquet on classpath

;; Reshape (datajure.reshape)
(require '[datajure.reshape :refer [melt]])
(melt ds {:id [:species :year] :measure [:mass :flipper :bill]})
(melt ds {:id [:species :year]})   ;; infer measure cols
(-> ds
    (melt {:id [:species :year] :measure [:mass :flipper]})
    (core/dt :by [:species :variable] :agg {:avg #dt/e (mn :value)}))

;; Joins (datajure.join)
(require '[datajure.join :refer [join]])
(join X Y :on :id :how :left)
(join X Y :on [:a :b] :how :inner)
(join X Y :left-on :id :right-on :key :how :left)
(join X Y :on :id :how :left :validate :m:1)
(join X Y :on :id :how :left :report true)
(-> (join X Y :on :id :how :left :validate :m:1 :report true)
    (core/dt :where #dt/e (> :year 2008)
             :agg {:total #dt/e (sm :mass)}))
```
