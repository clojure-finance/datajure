# Datajure v2 — Project Summary

## Overview

Datajure v2 is a Clojure data manipulation library for finance and empirical research. It provides a clean, composable query DSL (`dt`) built directly on **tech.ml.dataset** (`tech.v3.dataset`). The core abstraction is a reader-tagged expression `#dt/e` that compiles column expressions to vectorized dataset operations. Datajure v2 is a **syntax layer**, not an engine — it sits above tech.v3.dataset exactly as dplyr/data.table sit above R's data frames.

**Status**: Phase 1 complete. `datajure.expr` (AST + compiler + reader tag) is implemented and tested.

- v1 repo: https://github.com/clojure-finance/datajure
- v1 website: https://clojure-finance.github.io/datajure-website/

---

## Current Implementation State

### Phase 1 — AST foundation ✅ COMPLETE

**`src/datajure/expr.clj`** is the only implemented source file. It provides:

- AST node constructors: `col-node`, `lit-node`, `op-node`
- `parse-form` — Clojure form → AST tree
- `compile-expr` — AST → `fn [ds] → column/scalar`
- `->op-sym` — normalises op values: accepts symbols OR resolved fn objects (needed because nREPL resolves symbols before reader tags run)
- `read-expr` — reader tag handler for `#dt/e`
- `register-reader!` — AOT/script fallback via `alter-var-root`
- Stub fns `sq`, `log`, `and`, `or`, `not` — shadow `clojure.core` macros/fns so `#dt/e` literals work at the REPL when `:refer`'d

**`resources/data_readers.clj`** — primary registration: `{dt/e datajure.expr/read-expr}`

#### Known issue: `and`/`or`/`not` as REPL literals

`#dt/e (and ...)` written literally at the REPL fails because the Clojure compiler macro-expands `and`/`or` before the reader tag runs. Workarounds:
- `(read-string "#dt/e (and ...)")` works correctly
- Loading from `.clj` files works correctly
- **Fixed**: stub `defn`s shadow the macros when `:refer`'d into the user namespace

```clojure
(require '[datajure.expr :refer [and or not]])
#dt/e (and (> :mass 4000) (< :year 2010))  ; now works
```

`datajure.core` will `:refer` these automatically on require. In `.clj` files and `read-string` contexts no `:refer` is needed — the reader fires before the compiler.

#### Ops supported in compiler

| Symbol | dfn fn |
|---|---|
| `+`, `-`, `*`, `/` | `dfn/+`, `dfn/-`, `dfn/*`, `dfn//` |
| `>`, `<`, `>=`, `<=`, `=` | `dfn/>`, `dfn/<`, `dfn/>=`, `dfn/<=`, `dfn/eq` |
| `and`, `or`, `not` | `dfn/and`, `dfn/or`, `dfn/not` |
| `sq`, `log` | `dfn/sq`, `dfn/log` |

#### Nil-safety (implemented)

- Comparisons with nil literal → `boolean-array` of `false` (all rows)
- Arithmetic with nil literal → `nil` (becomes missing in dataset)
- Dataset columns with missing values handled natively by `dfn`

#### Require warnings (expected, harmless)

```
WARNING: and already refers to: #'clojure.core/and in namespace: datajure.expr, being replaced by: #'datajure.expr/and
```
These appear on `(require '[datajure.expr ...])`. Intentional — stub fns shadow core macros/fns.

### Phases 2–4 — NOT YET STARTED

`datajure.core` (the `dt` function) does not exist yet.

---

## Build Plan

### Phase 1 — AST foundation ✅
1. ✅ AST node types + `compile-expr` turning `#dt/e` forms into `dfn` calls
2. ✅ Nil-safety in compiler (comparisons → false, arithmetic → nil)
3. ✅ Register `#dt/e` reader tag via `data_readers.clj` + `alter-var-root` fallback

### Phase 2 — `dt` core, no grouping
4. `dt` with `:where` only (plain fns + `#dt/e`)
5. Add `:set` (column derivation, no `:by`)
6. Add `:agg` (whole-table summary, no `:by`)
7. Add `:select` and `:order-by`

### Phase 3 — grouping
8. Add `:by` + `:agg` (group aggregation)
9. Add `:by` + `:set` (window mode / grouped derivation)
10. Add `:set` + `:agg` conflict detection (throw error)

### Phase 4 — wire up
11. Enforce fixed evaluation order across all keyword combinations
12. Add `and`/`or`/`not`/`in`/`between?` special forms to compiler

---

## Project Structure

```
datajure/
├── deps.edn
├── PROJECT_SUMMARY.md
├── resources/
│   └── data_readers.clj          # {dt/e datajure.expr/read-expr}
└── src/
    └── datajure/
        └── expr.clj              # ✅ AST + compiler + reader tag (Phase 1)
        ;; TO BUILD:
        ;; core.clj               # dt query function
        ;; join.clj               # join, cross-join, anti-join
        ;; reshape.clj            # pivot-wider, pivot-longer / melt
        ;; window.clj             # win/* window functions
        ;; concise.clj            # short aliases: mn, sm, N, grp, gi
        ;; io.clj                 # unified file I/O
        ;; util.clj               # data cleaning utilities
```

---

## Dependencies

| Dependency | Version | Role |
|---|---|---|
| `org.clojure/clojure` | 1.12.4 | Language runtime |
| `techascent/tech.ml.dataset` | 8.003 | Core dataset engine (columnar, JVM) |
| `nrepl/nrepl` (alias) | 1.5.2 | REPL server on port 7888 |

**Key architectural decision**: Built directly on `tech.v3.dataset`, NOT on `tablecloth`. Tablecloth is redundant — Datajure v2 independently provides everything tablecloth offers.

---

## Core Syntax (Spec — not yet implemented beyond Phase 1)

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

**Critical constraint**: `:set` + `:agg` in the same `dt` call is always an error. Use threading.

### Fixed evaluation order

1. `:where` — filter rows
2. `:set` OR `:agg` (mutually exclusive)
3. `:select` — keep listed columns
4. `:order-by` — sort final output

---

## The `#dt/e` Reader Tag

### How it works

`(require '[datajure.expr])` triggers `data_readers.clj` which registers `read-expr` as the `dt/e` tag handler. `#dt/e` then rewrites forms to AST maps at read time. `dt` (once built) interprets ASTs at query time.

### REPL usage

```clojure
(require '[datajure.expr :as expr :refer [and or not sq log]])

;; Produces AST map
#dt/e (> :mass 4000)
;; => {:node/type :op, :op/name #object[clojure.core$_GT_ ...], :op/args [...]}
;; Note: :op/name is the resolved fn object at REPL; ->op-sym normalises this

;; and/or/not work as REPL literals once :refer'd
#dt/e (and (> :mass 4000) (< :year 2010))  ; works

;; Compile and run
(let [f (expr/compile-expr #dt/e (/ :mass (sq :height)))]
  (f my-dataset))
```

### Nil-safety rules (inside `#dt/e`)

1. **Comparisons** with nil → `false` (never throws)
2. **Arithmetic** with nil → nil propagates
3. **`coalesce`** → replace nil with fallback
4. **`pass-nil`** → wrap plain functions (to be implemented)

### Special forms (Phase 2+)

| Form | Compiles to |
|---|---|
| `(and ...)`, `(or ...)`, `(not ...)` | `dfn/and`, `dfn/or`, `dfn/not` |
| `(in :col #{...})` | Set membership test |
| `(between? :col lo hi)` | `(and (>= col lo) (<= col hi))` |
| `(cond pred val ... :else val)` | Chain of vectorized ternary nodes |
| `(let [sym expr ...] body)` | Named intermediate AST nodes |
| `(win/rank :col)` etc. | Window operations |
| `(row/sum :a :b ...)` etc. | Cross-column row-wise ops |

---

## Design Principles

1. `dt` is a function — not a macro
2. `dt` is a query form — joins/reshape/IO are separate composable functions
3. `:where` always filters — conditional updates go inside `:set`
4. Keyword lifting requires `#dt/e` — no implicit magic outside expression mode
5. Nil-safe by default — four-rule nil handling baked into `#dt/e` compiler
6. Expressions are values — `#dt/e` returns a reusable AST with preserved metadata
7. Two expression modes — `#dt/e` (vectorized, nil-safe) vs plain functions (flexible)
8. Polymorphic selectors — `:select` dispatch on argument type
9. Threading for pipelines — don't reinvent `->`, use it
10. Errors are data — structured `ex-info`, extensible multimethod
11. One function, not 28 — one `dt`, six keywords, two expression modes
12. Syntax layer, not engine — delegate to `tech.v3.dataset` and `dfn`

---

## Clarifications (Q&A from design sessions)

**`#dt/e` auto-registration**: `alter-var-root` on `*data-readers*` at load time via `data_readers.clj`. Same pattern as `clojure.instant` / `#inst`. Side-effectful by design; documented in namespace docstring. `(datajure/init!)` fallback for AOT/script edge cases.

**`grp`/`gi` scope**: v2.0 scope but deferred until priorities 1–10 are solid. Not v2.1, not blocking.

**`:set` + `:agg` in same call**: Always an error. The evaluation order list should be read as "whichever of `:set` or `:agg` is present runs at this stage" — they are mutually exclusive branches. Use `->` threading.

---

## Implementation Priority

| # | Feature | Status |
|---|---|---|
| 1 | `:by` × `:set`/`:agg` matrix | Phase 3 |
| 2 | `#dt/e` expression mode (nil-safety, `coalesce`, `let`, `cond`, `and`/`or`/`not`/`in`, `between?`) | Phase 1 partial ✅ |
| 3 | Polymorphic `:select` and `:by` | Phase 2 |
| 4 | `pass-nil` wrapper | Phase 4 |
| 5 | REPL-first: pretty-print + `*dt*` + Clerk | Later |
| 6 | Structured error messages | Later |
| 7 | `:within-order` for window ordering | Phase 3 |
| 8 | `asc`/`desc` sort helpers | Phase 2 |
| 9 | Reusable `#dt/e` expressions in vars | Later |
| 10 | `win/*` functions (incl. `win/rleid`) | Phase 3+ |
| 11 | `row/*` functions | Later |
| 12 | `count-distinct` | Later |
| 13 | `datajure.concise` | Later |
| 14 | Vector-of-pairs sequential semantics | Phase 2+ |
| 15 | `datajure.io` | Later |
| 16 | `datajure.util` | Later |
| 17 | Join `:validate` and `:report` | Later |
| 18–26 | v2.1 / v3 features | Future |
