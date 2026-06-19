# Changelog

All notable changes to Datajure will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.3.0] - 2026-06-19

### Added

- **Element-wise non-finite cleaners + stable `asinh` in `#dt/e`.** Four new element-wise ops, usable in `#dt/e` and in `:where`/`:agg`/`:set` data-forms: `na2zero` (non-finite — nil/NaN/±Inf — → 0.0), `nonfin2na` (non-finite → nil), `neg2na` (negative → nil; nil/non-finite pass through as nil), and `asinh` (numerically-stable inverse hyperbolic sine `sign(x)·ln(|x|+√(x²+1))`, nil for non-finite). These mirror mbmisc's `na2zero`/`nonfin2na`/`neg2na` and a stable `asinh` — the textbook `ln(x+√(x²+1))` form silently collapses large-negative inputs to nil. The scalar primitives `math/finite-double?` and `math/asinh` back them.
- **`win/grr` — inverse-hyperbolic-sine growth window op.** `#dt/e (win/grr :x)` computes `asinh(x[i]) − asinh(x[i-1])` per partition/order (mbmisc `grr` with IHS=TRUE): nil for the first element, and a run of zeros (`x[i]==0 && x[i-1]==0`) yields `0.0` rather than `asinh(0)−asinh(0)`. Like other `win/*` ops it runs in `:set` window mode (`:by` + optional `:within-order`).
- **Multi-quantile `qnt` — one sort for a whole band set.** `(qnt :col [0.2 0.5 0.8])` (and `core/qnt`/`concise/qnt`, and the data-form `[:qnt :col [0.2 0.5 0.8]]`) returns a vector of quantiles, sorting the column **once** instead of three times — the q20/median/q80 idiom in one agg. Backed by `math/quantiles-type7`. In a data-form, a number-headed vector like `[0.2 0.5 0.8]` is now a literal (so the form works), while a non-number-headed vector stays an operation.

### Changed

- **`:by`/`:agg` group aggregation is dramatically faster at scale.** The group-by path no longer builds a materialised sub-dataset per group (over *all* columns) and concatenates one-row results; it now (1) narrows the dataset to just the columns the aggs reference, (2) groups row indices in a single key-column pass, and (3) for single-column quantile aggregators (`qnt`/`md`) gathers each group's values into a primitive `double[]` and reduces with no boxing — assembling the result columns once. On a real 2.1M-row × 20.8k-group cross-section (69 industries × q20/median/q80), the canonical Fama-French/peer-bands aggregation went from **>5 min (effectively unusable) to ~12 s**. Pure internal change — same results (verified against the prior path across the suite); other aggregators and `within-order` groups keep the general per-group path.

### Fixed

- **`qnt`/`md` on a date/temporal column now throw a structured `:quantile-non-numeric` error** instead of a raw `ClassCastException` (`LocalDate cannot be cast to Number`) deep in the sort — the same class of guard as the as-of `:asof-non-numeric-asof-key`. Convert the column to epoch days/millis to rank it.

## [2.2.0] - 2026-06-19

### Added

- **R type-7 quantiles everywhere — `median`, `qtile`/`cut`, `winsorize`, `describe` now match R exactly.** datajure's quantile/median estimator was tech.ml.dataset's `dfn/percentiles` / `dfn/median` (Apache Commons Math), which disagrees with R's default **type-7** at the tails *and*, for some n, at the median (e.g. median of a 542-value column: `57.999` vs R's `57.9375`). All quantile call-sites now use a single type-7 primitive (`datajure.math/quantile-type7`): `core/median`, the `qtile`/`cut` breakpoints, `stat/winsorize`, and `util/describe` quartiles. Quantiles drop **nil and non-finite** (NaN/±Inf, R's `is.finite`) before estimating. This is a deliberate behavior change for parity with R as the reference implementation (no backwards-compat shim — see the project's quantile decision).
- **`qnt` — type-7 quantile aggregator in `#dt/e`.** `#dt/e (qnt :col p)` returns the type-7 p-quantile (p a fraction in [0,1]) of a column's finite values, e.g. `(dt panel :by [:gind] :agg {:q20 #dt/e (qnt :saleq 0.2) :med #dt/e (qnt :saleq 0.5)})`. A 3-arg form `(qnt :col p min-n)` returns nil when fewer than `min-n` finite values remain (floor-free by default — `min-n` is opt-in for rules like peer-bands n≥11). `md` is now type-7 too, so `(md :col)` and `(qnt :col 0.5)` agree.
- **`:agg` / `:set` accept the runtime data-form vector (parity with `:where`).** A plain vector `[op-kw & args]` now works in `:agg` and `:set`, not just `:where`: `(dt ds :by [:g] :agg {:q20 [:qnt :x 0.2]})`, `(dt ds :set {:r [:div0 [:- :a :b] :a]})`. Keywords are columns, vectors are operations, anything else a literal — desugaring to the same AST `#dt/e` compiles, so it runs the vectorized path. This makes programmatic query generation ergonomic (e.g. generate one `qnt` aggregation per column in a list) without reaching for `read-expr` over a quoted form. `:agg`/`:set` data-forms also allow the scalar aggregators (`mn`/`sm`/`md`/`qnt`/…); `:where` data-forms stay element-wise-only (a predicate can't be an aggregator). Unknown ops throw `:unknown-data-op`.

### Fixed

- **`dio/read-seq` now applies the string→keyword `:column-allowlist` / `:column-blocklist` normalization on Parquet (the 2.0.13 fix had reached `dio/read` only).** Parquet applies `:key-fn` to column names *before* matching the allow/block list, so under datajure's keyword `:key-fn` a **string** allowlist entry silently matched nothing → a 0-column dataset on the streaming reader (the dashboard was unaffected — it uses `dio/read`; this bit only the out-of-core `read-seq` path). `read-seq`'s Parquet branch now defaults `:key-fn keyword` and normalizes string entries to keywords just like `read`, so `{:column-allowlist [:a :b]}` and `{:column-allowlist ["a" "b"]}` both work on both readers. Verified end-to-end against a real prepared Parquet.
- **`:direction :nearest` / `:how :window` on a date/temporal asof key now throw a structured error instead of a raw `ClassCastException`.** Both paths rank or bound matches by raw arithmetic on the asof value, which a `java.time` key (e.g. `:local-date`) cannot supply, so they previously failed deep in the search with an opaque `class java.time.LocalDate cannot be cast to class java.lang.Number`. They now raise `:dt/error :asof-non-numeric-asof-key` up front, naming the offending columns/datatypes and pointing to the supported paths (`:direction :backward`/`:forward`, and `:tolerance` via `[n unit]`, all of which keep working with temporal keys) or to converting the column to epoch days/millis. A packed date type (`:packed-local-date`) reports as both numeric and datetime but yields temporal objects, so it is correctly treated as temporal here.

## [2.1.0] - 2026-06-17

### Added

- **`:where` data-form predicate — vectorized filtering by a runtime value.** `:where` now accepts a plain data vector where a keyword is a column and anything else is a literal value, so a runtime value flows straight in: `(dt panel :where [:= :tic ticker])`, `(dt ds :where [:and [:>= :mass lo] [:< :mass hi]])`. It desugars to the same AST `#dt/e` compiles, so it runs the vectorized `dfn` path — no per-row row-map (the footgun of a plain-fn `:where` on a wide dataset). `#dt/e` is read-time and can't see a runtime local; the data-form is its runtime-data twin, ideal for parameterized queries and screens built programmatically. Supports the element-wise ops `> < >= <= = and or not in between? + - * / sq log div0` (use a set for `:in`); aggregations, window/row/stat ops, and `if/cond/let/cut/xbar` stay `#dt/e`-only. Errors: `:unknown-data-op`, `:invalid-data-op`, and the usual `:unknown-column`.
- **As-of `:tolerance` accepts temporal asof keys — point-in-time staleness caps.** An as-of join on a date/time key (e.g. CRSP daily `date` × Compustat `rdq`) can now bound how stale a match may be with a `[n unit]` duration: `(join crsp compustat :left-on [:gvkey :date] :right-on [:gvkey :rdq] :how :asof :tolerance [90 :days])` keeps each daily row matched to the most recent report no older than 90 days, nil beyond that. Units `:seconds`/`:minutes`/`:hours`/`:days`/`:weeks` (the temporal difference is taken in epoch-milliseconds). Numeric asof keys keep their plain-number `:tolerance`.
- **`datajure.index` — keyed lookup index for repeated point-lookups.** Build an index once over one or more key columns, then resolve a key to its rows in O(1) instead of scanning: `(def by-tic (idx/index-by panel :tic))` then `(idx/lookup by-tic "AAPL")` → sub-dataset in original row order. Multi-column keys use a tuple; an absent key yields an empty dataset; `lookup-indices` returns the raw row indices for gathering from a row-aligned projection yourself. The index is an immutable value holding a reference to its source dataset, so a lookup can't be applied to a mismatched table — data.table's `setindex()`, never a mutating `setkey()`. Adds `index?` / `kind` / `key-columns` / `source-dataset`; structured errors `:unknown-column` / `:invalid-key-cols` / `:invalid-lookup-key` / `:not-an-index`.
- **`:asof` index kind, reusable across as-of / window joins.** `(idx/index-by right key-cols {:kind :asof})` (or `idx/asof-index`) builds the prepared right-side structure that as-of and window joins use internally (last key column = asof column, the rest exact-match). The same index value can be reused across repeated merges against one right table — e.g. CRSP daily × Compustat quarterly under several reporting-lag assumptions — instead of rebuilding it each call. The build logic now lives in `datajure.index`; `datajure.asof` owns only the search and result assembly.

### Changed

- **As-of / window joins: O(left × group-size) → O(left × log group-size) per left row.** `datajure.asof`'s right-side index now splits each group's as-of values and original row indices (and wraps the column reader) once per group, instead of rebuilding them on every left row. `asof-match` / `window-indices` now do only a map lookup plus a binary search per probe. Pure internal change — same results, same public API. Verified depth-independent: 200k left rows hold ~140 ms as the matched group's depth grows 100→1600.
- **`asof-match` / `window-indices` take an opts map.** The canonical arity is now `(asof-match left right left-keys right-keys {:direction … :tolerance … :right-index …})` (and `{:lo :hi :right-index}` for `window-indices`), avoiding a long positional signature ahead of further as-of options. Existing positional arities are kept and delegate, so callers are unaffected.
- **As-of result assembly is array-based — lower peak memory at scale.** The as-of join now builds its result from primitive `long[]` row-index arrays (`asof-row-indices`) and gathers each right column into a single object-array (`build-result-arrays`), instead of materialising a sequence of `[left right]` pair vectors and all right columns at once. Peak working memory drops from all-right-columns-wide to one-column-wide, and no per-row pair objects are allocated — meaningful on wide many-row joins (e.g. CRSP daily × Compustat quarterly). `asof-match` / `build-result` keep their pair-sequence contracts as thin wrappers. Right-column datatypes and missing values are preserved exactly (verified by regression).
- **`:tolerance` on a temporal asof key now requires a `[n unit]` spec.** Previously any `:tolerance` on a non-numeric (e.g. datetime) asof key threw `:join-tolerance-non-numeric`; temporal keys are now supported, so a bare number on a temporal key throws `:join-tolerance-invalid` (directing to `[n unit]`), an unknown unit throws `:join-tolerance-unknown-unit`, and `:join-tolerance-non-numeric` is now reserved for keys that are neither numeric nor temporal (e.g. strings).

## [2.0.13] - 2026-06-16

### Added

- **`:take` — row limit in `dt`.** Positive `n` keeps the first `n` rows (head), negative keeps the last `|n|` (tail), `0` yields no rows, and `|n|` beyond the row count returns all rows. It runs last, after `:order-by`, so the everyday "last 20 by date" is one query: `(dt ds :order-by [(asc :date)] :take -20)` — no more dropping out to `(ds/tail 20 …)`. (The signed convention follows q's `#`.) A non-integer `:take` throws `:dt/error :invalid-take`.

### Fixed

- **`dio/read` accepts string `:column-allowlist` / `:column-blocklist` for Parquet/Arrow.** Parquet/Arrow apply `:key-fn` to column names *before* matching the allow/block list, so under datajure's keyword `:key-fn` a **string** entry silently matched nothing → a 0-column dataset (the mirror image of the CSV/TSV bug fixed in 2.0.12). `dio/read` now normalises string entries to keywords for Parquet/Arrow, so `{:column-allowlist [:a :b]}` and `{:column-allowlist ["a" "b"]}` both work on every format. (Parquet reading itself was already supported; this closes the allowlist asymmetry.)

## [2.0.12] - 2026-06-16

### Added

- **`core/div0` — nil-safe division as a callable function.** `div0` was an `#dt/e`-only op, so plain-fn contexts (`:set`/`:agg` with `#(...)`, computed `:by`) couldn't reach it and had to roll their own zero-guard. It's now also a public scalar fn: `(div0 num den)` → `nil` when either is nil or the denominator is zero, else `num`/`den` as a double; non-numeric inputs throw normally. The `#dt/e` `div0` op delegates to the same fn (single source of truth via `expr/div0`).
  ```clojure
  (div0 1 2)   ;; => 0.5
  (div0 1 0)   ;; => nil
  (dt ds :set {:pe #(div0 (:price %) (:earnings %))})  ;; plain-fn :set
  ```

### Fixed

- **`dio/read` accepts keyword `:column-allowlist` / `:column-blocklist` for CSV/TSV.** charred matches these against the raw header strings *before* `:key-fn` is applied, so a keyword allowlist — the natural form, since datajure keyword-izes columns — silently matched nothing and returned a **0-column dataset**. `dio/read` now normalises keyword entries to their raw names for CSV/TSV, so `{:column-allowlist [:a :b]}` and `{:column-allowlist ["a" "b"]}` both work. Scoped to CSV/TSV; Parquet/Arrow match after `:key-fn`, so they're left untouched.

- **`#dt/e (div0 …)` with scalar operands now returns a scalar.** It always built a length-1 column, so a composed expression like `#dt/e (+ :x (div0 1 2))` produced `[1.5 nil nil]` instead of `[1.5 2.5 3.5]` (the length-1 reader only covered row 0). It now returns a scalar when both operands are scalars — consistent with the other arithmetic ops — so it broadcasts correctly. Column operands are unchanged.

- **`:order-by` / `:within-order` now validate their columns.** An unknown sort column previously surfaced a raw tech.ml.dataset `Column not found` exception; it now throws datajure's structured `:dt/error :unknown-column` (with `:dt/context :order-by`/`:within-order` and Damerau-Levenshtein suggestions), the same as `:select`. `:order-by` is validated against the post-transform dataset, so sorting by a `:set`-derived column works and sorting by a `:select`-dropped column errors. A malformed spec (missing `:col`, or `:order` that isn't `:asc`/`:desc`) throws `:dt/error :invalid-order-spec`.

- **`and`/`or` in `#dt/e` are now variadic.** They compiled directly to `dfn/and`/`dfn/or`, which are binary-only, so `#dt/e (and p1 p2 p3)` threw `Wrong number of args (3)`. They now fold over `dfn/and`/`dfn/or`, accepting any number of predicates: `#dt/e (and (> :a 1) (> :b 2) (< :c 3))`. (`not` remains unary; zero-arity `(and)`/`(or)` is rejected at read time with a `:wrong-arity` error, since it's meaningless in a vectorized mask.)

### Changed

- **`:order-by` / `:within-order` are dramatically faster on wide datasets.** Sorting routed through tech.ml.dataset's `(sort-by dataset identity comparator)`, whose `identity` key-fn forces a full row object to be materialised for *every* row — all columns — even though only the sort keys are compared. On a 2.1M-row × 91-column dataset, sorting by two keys took **~326 s**; it now takes **~1.6 s** (~200×). The sort now reads only the key columns, stable-sorts an index permutation with `clojure.core/compare` (nils first, mixed `:asc`/`:desc`), and gathers via `ds/select-rows`. Ordering semantics are unchanged — including nil placement, tie stability, and decoded-value comparison for packed columns like dates.

## [2.0.11] - 2026-06-14

### Added

- **Max/min aggregation inside `#dt/e` (`mx` / `mi`).** `#dt/e` previously had no max/min op at all, so the canonical OHLC example `:hi #dt/e (mx :price)` threw `Unknown op` at read time (uncaught because the OHLC tests only asserted open/close, never hi/lo). `mx`/`mi` aggregate a column's maximum/minimum, skipping nil and returning nil for an all-missing column.
  ```clojure
  (dt trades :by [:sym] :within-order [(asc :time)]
      :agg {:open #dt/e (first-val :price) :close #dt/e (last-val :price)
            :hi   #dt/e (mx :price)        :lo    #dt/e (mi :price)})
  ```

- **Full-name and concise aggregation names now work inside `#dt/e`.** Every `datajure.core` / `datajure.concise` aggregation helper is accepted as a `#dt/e` op, in both spellings — so `#dt/e (max* :price)`, `#dt/e (sum :size)`, `#dt/e (count* :revenue)`, `#dt/e (nuniq :id)` etc. all work, not just the concise `mn`/`sm`/`md`/`sd`. New ops `:variance` and `:ct` (non-nil count, also backing `core/count*`).

### Fixed

- **`core/max*` and `core/min*` returned wrong values on columns with missing data.** They aliased `dfn/reduce-max` / `dfn/reduce-min`, which corrupt the reduction when a missing value precedes the extremum (e.g. `max*` of `[5000 3750 nil 4000]` returned `4000`, not `5000`). They now filter nils first via the shared `expr/col-max` / `expr/col-min`, skipping missing and returning `nil` for an all-missing column; the `#dt/e` `mx`/`mi` ops use the same correct path.

### Developer experience

- **`wavg`/`wsum` wrong-arity error is now structured.** Calling `#dt/e (wavg :x)` (or `wsum`, or the `wa`/`ws` aliases) with other than two arguments now throws a structured `:wrong-arity` `ex-info` at read time — *"`wavg` takes exactly two arguments (weight column, value column). Got 1."* — instead of a raw `clojure.lang.ArityException`.

- **Clear error for a literal nil in arithmetic.** `#dt/e (+ :x nil)` (and `-` / `*` / `/` / `sq` / `log`) now throws a structured `:arith-nil-literal` error at read time — *"Arithmetic op `+` received a literal nil … use `coalesce` … or `div0` …"* — instead of a cryptic `"Item type null has no iterator"` from deep in the dataset layer. Arithmetic requires non-nil operands (nil is ambiguous there); predicates keep their nil-literal → `false` rule, and `coalesce`/`div0` remain the tools for handling nils. (Earlier docs claimed arithmetic-with-a-nil-literal returned nil; it never did — it threw.)

## [2.0.10] - 2026-06-14

### Added

- **JSON I/O in `datajure.io`.** `.json` (and `.json.gz`) is now a natively supported format for `read` and `write`, alongside CSV/TSV/Nippy — no optional dependency required (`tech.v3.dataset` parses/emits JSON via charred). Files are read/written as a JSON array of row objects; column names round-trip as keywords like every other format.
  ```clojure
  (dio/read "data.json")
  (dio/write ds "output.json")
  (dio/read "data.json.gz")   ;; gzip auto-detected
  ```
  `read-seq` also accepts `.json`: since a JSON array is a single document with no chunk boundaries, it is read whole and yielded as a **one-element** lazy sequence (API uniformity across formats — no streaming/memory benefit; use Parquet or JSON Lines for true out-of-core reads).

- **JSON Lines I/O (`.jsonl` / `.ndjson`) in `datajure.io`.** Newline-delimited JSON — one object per line — for `read`, `write`, and `read-seq` (and `.gz` variants). Unlike a JSON array, JSON Lines is genuinely chunk-able, so `read-seq` **streams** it in batches of `:batch-size` rows (default 100000), suitable for files larger than memory. Parsing/encoding uses charred (the same library tech.v3.dataset uses), now declared as an explicit dependency. Missing keys read back as `nil`; blank lines are skipped.
  ```clojure
  (dio/read  "events.jsonl")
  (dio/write ds "events.jsonl")
  (doseq [chunk (dio/read-seq "huge.jsonl" {:batch-size 50000})]   ;; streamed
    (process chunk))
  ```

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

[Unreleased]: https://github.com/clojure-finance/datajure/compare/v2.3.0...HEAD
[2.3.0]: https://github.com/clojure-finance/datajure/compare/v2.2.0...v2.3.0
[2.2.0]: https://github.com/clojure-finance/datajure/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/clojure-finance/datajure/compare/v2.0.13...v2.1.0
[2.0.13]: https://github.com/clojure-finance/datajure/compare/v2.0.12...v2.0.13
[2.0.12]: https://github.com/clojure-finance/datajure/compare/v2.0.11...v2.0.12
[2.0.11]: https://github.com/clojure-finance/datajure/compare/v2.0.10...v2.0.11
[2.0.10]: https://github.com/clojure-finance/datajure/compare/v2.0.9...v2.0.10
[2.0.9]: https://github.com/clojure-finance/datajure/compare/v2.0.8...v2.0.9
[2.0.8]: https://github.com/clojure-finance/datajure/compare/v2.0.7...v2.0.8
[2.0.7]: https://github.com/clojure-finance/datajure/compare/v2.0.6...v2.0.7
[2.0.6]: https://github.com/clojure-finance/datajure/compare/v2.0.5...v2.0.6
