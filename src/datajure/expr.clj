(ns datajure.expr
  "AST definition and compiler for #dt/e expressions.

  #dt/e is a reader tag that produces an AST map. datajure.core interprets
  these ASTs when executing dt queries. This namespace handles:
    - AST node constructors
    - compile-expr: AST -> fn of dataset -> column/scalar
    - Reader tag handler registered via resources/data_readers.clj (primary)
      and register-reader! / alter-var-root (AOT/script fallback)

  Op names are stored as keywords in the AST (e.g. :and, :>, :+) rather than
  symbols, because the Clojure compiler tries to resolve symbols in literal
  data structures. Since and/or/not are macros, the compiler rejects
  'Can't take value of a macro' when it encounters them as bare symbols in
  the map values returned by the reader tag. Keywords are self-evaluating
  and avoid this entirely.

  Nil-safety rules (matching spec):
    - Comparison ops with nil arg -> false column (all rows false)
    - Arithmetic ops with nil arg -> nil (becomes missing when stored in dataset)
  These rules only activate when a Clojure nil literal appears in an expression.
  Dataset columns with missing values are handled natively by dfn; :object
  readers from nil-producing element-wise ops (date-parts, cleaners) get the
  same treatment via nil-safe-cmp — a nil element compares false."
  (:require [clojure.string :as str]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.dataset :as ds]
            [datajure.window :as win]
            [datajure.row :as row]
            [datajure.stat :as stat]
            [datajure.math :as math]))

;; ---------------------------------------------------------------------------
;; Op dispatch table: symbol -> keyword -> dfn fn
;; ---------------------------------------------------------------------------

(def ^:private comparison-ops #{:> :< :>= :<= := :and :or :not :in :between?})

(def ^:private win-sym->op
  "Maps win/* source symbols to canonical keyword op names."
  {'win/rank :win/rank
   'win/dense-rank :win/dense-rank
   'win/row-number :win/row-number
   'win/lag :win/lag
   'win/lead :win/lead
   'win/tlag :win/tlag
   'win/cumsum :win/cumsum
   'win/cummin :win/cummin
   'win/cummax :win/cummax
   'win/cummean :win/cummean
   'win/rleid :win/rleid
   'win/delta :win/delta
   'win/ratio :win/ratio
   'win/differ :win/differ
   'win/mavg :win/mavg
   'win/msum :win/msum
   'win/mdev :win/mdev
   'win/mdowndev :win/mdowndev
   'win/mmin :win/mmin
   'win/mmax :win/mmax
   'win/ema :win/ema
   'win/fills :win/fills
   'win/bfill :win/bfill
   'win/grr :win/grr})

(def ^:private win-op-table
  "Maps window op keywords to runtime functions from datajure.window."
  {:win/rank win/win-rank
   :win/dense-rank win/win-dense-rank
   :win/row-number win/win-row-number
   :win/lag win/win-lag
   :win/lead win/win-lead
   :win/tlag win/win-tlag
   :win/cumsum win/win-cumsum
   :win/cummin win/win-cummin
   :win/cummax win/win-cummax
   :win/cummean win/win-cummean
   :win/rleid win/win-rleid
   :win/delta win/win-delta
   :win/ratio win/win-ratio
   :win/differ win/win-differ
   :win/mavg win/win-mavg
   :win/msum win/win-msum
   :win/mdev win/win-mdev
   :win/mdowndev win/win-mdowndev
   :win/mmin win/win-mmin
   :win/mmax win/win-mmax
   :win/ema win/win-ema
   :win/fills win/win-fills
   :win/bfill win/win-bfill
   :win/grr win/win-grr})

(defn win-op-fn
  "The runtime window fn for a `:win/<op>` keyword, or nil. Lets callers (e.g. the
  core group-set fast path) apply a single-column window op directly to a column
  slice without compiling/running the whole AST."
  [win-op-kw]
  (win-op-table win-op-kw))

(def ^:private row-sym->op
  "Maps row/* source symbols to canonical keyword op names."
  {'row/sum :row/sum
   'row/mean :row/mean
   'row/min :row/min
   'row/max :row/max
   'row/count-nil :row/count-nil
   'row/any-nil? :row/any-nil?})

(def ^:private row-op-table
  "Maps row op keywords to runtime functions from datajure.row."
  {:row/sum row/row-sum
   :row/mean row/row-mean
   :row/min row/row-min
   :row/max row/row-max
   :row/count-nil row/row-count-nil
   :row/any-nil? row/row-any-nil?})

(def ^:private stat-sym->op
  "Maps stat/* source symbols to canonical keyword op names."
  {'stat/standardize :stat/standardize
   'stat/demean :stat/demean
   'stat/winsorize :stat/winsorize
   'stat/trim :stat/trim
   'stat/rescale :stat/rescale})

(def ^:private stat-op-table
  "Maps stat op keywords to runtime functions from datajure.stat."
  {:stat/standardize stat/stat-standardize
   :stat/demean stat/stat-demean
   :stat/winsorize stat/stat-winsorize
   :stat/trim stat/stat-trim
   :stat/rescale stat/stat-rescale})

(defn count-distinct
  "Count of distinct non-nil values in a column."
  [col]
  (count (distinct (filter some? (dtype/->reader col)))))

(defn first-val
  "First value in a column."
  [col]
  (first (dtype/->reader col)))

(defn last-val
  "Last value in a column."
  [col]
  (let [r (dtype/->reader col)]
    (nth r (dec (dtype/ecount r)))))

(defn- check-equal-lengths!
  "Throw a structured ex-info if weight and value readers have different
  lengths. Used by wavg/wsum to turn a silent-truncation / NPE bug into a
  clear error naming the caller."
  [op-name wr vr]
  (let [nw (dtype/ecount wr)
        nv (dtype/ecount vr)]
    (when (not= nw nv)
      (throw (ex-info (str op-name
                           " requires weight and value columns of equal length, got "
                           nw " and " nv ".")
                      {:dt/error :unequal-column-lengths
                       :dt/op op-name
                       :dt/weight-length nw
                       :dt/value-length nv})))))

(defn wavg
  "Weighted average. Args: weight-col value-col. Skips nil pairs.
  Throws :unequal-column-lengths when w and v have different lengths."
  [w v]
  (let [wr (dtype/->reader w)
        vr (dtype/->reader v)
        _ (check-equal-lengths! "wavg" wr vr)
        n (dtype/ecount wr)
        pairs (keep (fn [i]
                      (let [wi (nth wr i) vi (nth vr i)]
                        (when (and (some? wi) (some? vi)) [wi vi])))
                    (range n))]
    (if (empty? pairs)
      nil
      (let [wvs (map (fn [[wi vi]] (* wi vi)) pairs)
            ws (map first pairs)]
        (/ (reduce + wvs) (reduce + ws))))))

(defn wsum
  "Weighted sum. Args: weight-col value-col. Skips nil pairs.
  Throws :unequal-column-lengths when w and v have different lengths."
  [w v]
  (let [wr (dtype/->reader w)
        vr (dtype/->reader v)
        _ (check-equal-lengths! "wsum" wr vr)
        n (dtype/ecount wr)
        wvs (keep (fn [i]
                    (let [wi (nth wr i) vi (nth vr i)]
                      (when (and (some? wi) (some? vi)) (* wi vi))))
                  (range n))]
    (if (empty? wvs) nil (reduce + wvs))))

(defn col-max
  "Column maximum, skipping nil/missing; nil for an all-missing column.
  `dfn/reduce-max` corrupts the reduction when the column has missing values
  (a missing slot poisons the running max), so we filter first and use
  `clojure.core/max`. Backs both the #dt/e `:mx` op and `core/max*`."
  [col]
  (let [vs (remove nil? (dtype/->reader col))] (when (seq vs) (apply max vs))))

(defn col-min
  "Column minimum, skipping nil/missing; nil for an all-missing column.
  See [[col-max]] for why `dfn/reduce-min` isn't used. Backs `:mi` and `core/min*`."
  [col]
  (let [vs (remove nil? (dtype/->reader col))] (when (seq vs) (apply min vs))))

(defn count-non-nil
  "Count of non-nil values in a column. Backs the #dt/e `:ct` op and `core/count*`."
  [col]
  (count (remove nil? (dtype/->reader col))))

(defn col-prod
  "Product of the non-nil values in a column; nil for an all-missing column
  (mbmisc `mb.prod` — NOT `prod`'s empty-product 1, which is misleading when
  every observation is missing). Backs the #dt/e `:prod` op and `core/prod`."
  [col]
  (let [vs (remove nil? (dtype/->reader col))]
    (when (seq vs) (reduce * vs))))

(defn div0
  "Nil-safe scalar division: returns nil when the numerator or denominator is nil,
  or the denominator is zero; otherwise `num` / `den` as a double. Non-numeric
  inputs throw normally — this guards data absence, not programmer errors.
  Backs both the #dt/e `:div0` op and `core/div0`."
  [num den]
  (when (and (some? num) (some? den) (not (zero? den)))
    (/ (double num) (double den))))

(defn- temporal-part
  "Element-wise nil-safe calendar-field extraction backing the #dt/e date-part
  ops (`year`/`month`/`day`/`dow`/`quarter`). `dtype-dt/long-temporal-field` alone
  leaks the packed missing-value sentinel (Long/MIN_VALUE) through missing date
  slots, so extract per element and yield nil where the date is missing. `xform`
  post-processes the extracted long (quarter = month -> 1..4). Works on date and
  date-time columns; raw instants lack calendar fields (zone-less) and throw
  java.time's UnsupportedTemporalTypeException — convert them to local dates first."
  ([field] (temporal-part field identity))
  ([field xform]
   (fn [col]
     (if (dtype/reader? col)
       (dtype/make-reader :object (dtype/ecount col)
                          (when-some [v (nth col idx)]
                            (xform (dtype-dt/long-temporal-field field v))))
       (when (some? col) (xform (dtype-dt/long-temporal-field field col)))))))

(defn- object-reader?
  "True for a reader/column whose elemwise datatype is :object — the shape the
  nil-producing element-wise ops (date-parts, non-finite cleaners) return before
  :set materialises them into a typed column with a missing bitmap."
  [x]
  (and (dtype/reader? x) (identical? :object (dtype/elemwise-datatype x))))

(defn- nil-safe-cmp
  "Ordering comparison that extends the DSL's nil rule (comparison with nil ->
  false) to :object readers: dfn's ordering predicates NPE on a nil element, so
  when either operand is an object reader, compare element-wise with nil -> false.
  Typed columns (where missing is a bitmap/NaN, handled natively by dfn) and
  scalars keep the vectorized `dfn-op` fast path."
  [dfn-op scalar-op]
  (fn [a b]
    (if (or (object-reader? a) (object-reader? b))
      (let [a-reader? (dtype/reader? a)
            b-reader? (dtype/reader? b)
            n (if a-reader? (dtype/ecount a) (dtype/ecount b))]
        (dtype/make-reader :boolean n
                           (let [x (if a-reader? (nth a idx) a)
                                 y (if b-reader? (nth b idx) b)]
                             (boolean (and (some? x) (some? y) (scalar-op x y))))))
      (dfn-op a b))))

(def ^:private gt-op (nil-safe-cmp dfn/> >))
(def ^:private lt-op (nil-safe-cmp dfn/< <))
(def ^:private gte-op (nil-safe-cmp dfn/>= >=))
(def ^:private lte-op (nil-safe-cmp dfn/<= <=))

(def ^:private op-table
  {:+ dfn/+
   :- dfn/-
   :* dfn/*
   :div (fn [a b] (dfn// (dfn/double a) (dfn/double b)))
   :div0 (fn [a b]
           ;; nil-safe division via the shared scalar `div0`. When both operands are
           ;; scalars, return a scalar (like the other arithmetic ops) so it
           ;; broadcasts in composed exprs e.g. (+ :x (div0 1 2)); otherwise build a
           ;; :float64 column element-wise (nil → NaN, which the dataset treats as
           ;; missing → reads back as nil).
           (let [a-reader? (dtype/reader? a)
                 b-reader? (dtype/reader? b)]
             (if (or a-reader? b-reader?)
               (let [n (if a-reader? (dtype/ecount a) (dtype/ecount b))]
                 (dtype/make-reader :float64 n
                                    (div0 (if a-reader? (nth a idx) a)
                                          (if b-reader? (nth b idx) b))))
               (div0 a b))))
   :sq dfn/sq
   :log dfn/log
   ;; elementary math (one-for-one dfn delegations, same nil story as sq/log):
   ;; abs/exp/sqrt/floor/ceil/signum are unary, pow is binary, round returns longs
   :abs dfn/abs
   :exp dfn/exp
   :sqrt dfn/sqrt
   :pow dfn/pow
   :floor dfn/floor
   :ceil dfn/ceil
   :round dfn/round
   :signum dfn/signum
   ;; element-wise non-finite cleaners (mbmisc na2zero/neg2na/nonfin2na) + stable
   ;; inverse-hyperbolic-sine. Per-element nil/NaN/±Inf handling → object readers.
   :asinh (fn [col] (dtype/make-reader :object (dtype/ecount col)
                                       (math/asinh (nth col idx))))
   :na2zero (fn [col] (dtype/make-reader :object (dtype/ecount col)
                                         (let [v (nth col idx)]
                                           (if (math/finite-double? v) (double v) 0.0))))
   :neg2na (fn [col] (dtype/make-reader :object (dtype/ecount col)
                                        (let [v (nth col idx)]
                                          (when (and (math/finite-double? v) (>= (double v) 0.0))
                                            (double v)))))
   :nonfin2na (fn [col] (dtype/make-reader :object (dtype/ecount col)
                                           (let [v (nth col idx)]
                                             (when (math/finite-double? v) (double v)))))
   ;; NA-propagating guard (R's ifelse(NA → NA) building block): the body value
   ;; where the guard is finite, nil (missing) where it is nil/NaN/±Inf. Brings
   ;; NA-propagating binary indicators (Piotroski-style p-ind) into the DSL —
   ;; a bare comparison collapses a nil operand to false *before* `if` sees it,
   ;; so `(if (>= :g 0) 1.0 0.0)` alone can't yield nil; guard on the input:
   ;;   #dt/e (when-finite :g (if (>= :g 0) 1.0 0.0))
   :when-finite (fn [guard body]
                  ;; a non-numeric non-nil guard element would ClassCastException
                  ;; inside finite-double? — surface a structured error instead:
                  ;; when-finite is a NUMERIC guard, not a general presence guard
                  (let [finite? (fn [v]
                                  (cond
                                    (nil? v) false
                                    (number? v) (math/finite-double? v)
                                    :else
                                    (throw (ex-info
                                            (str "when-finite requires a NUMERIC guard "
                                                 "expression (nil/NaN/±Inf → missing); got a "
                                                 (.getName (class v)) " (" (pr-str v) "). "
                                                 "For a non-numeric presence guard, use a "
                                                 "plain-fn derivation with pass-nil.")
                                            {:dt/error :when-finite-non-numeric
                                             :dt/value-class (.getName (class v))}))))]
                    (if (dtype/reader? guard)
                      (dtype/make-reader :object (dtype/ecount guard)
                                         (when (finite? (nth guard idx))
                                           (if (dtype/reader? body) (nth body idx) body)))
                      (when (finite? guard) body))))
   ;; calendar date-parts (data.table year()/month(), Polars .dt.*): element-wise
   ;; long extraction from date/date-time columns, nil for missing dates. dow is
   ;; ISO/java.time (1=Monday..7=Sunday); quarter derives 1..4 from the month.
   :year (temporal-part :years)
   :month (temporal-part :months)
   :day (temporal-part :days)
   :dow (temporal-part :day-of-week)
   :quarter (temporal-part :months (fn [m] (inc (quot (dec (long m)) 3))))
   :> gt-op
   :< lt-op
   :>= gte-op
   :<= lte-op
   := dfn/eq
   ;; dfn/and and dfn/or are binary-only, so fold to support (and a b c ...)
   :and (fn [& args] (reduce dfn/and args))
   :or (fn [& args] (reduce dfn/or args))
   :not dfn/not
   :mn dfn/mean
   :sm dfn/sum
   ;; R type-7 (matches R's median; differs from dfn/median for some n). qnt at
   ;; p=0.5 and md agree by construction.
   :md (fn [col] (math/quantile-type7 col 0.5))
   ;; type-7 p-quantile aggregator. (qnt :col p) or (qnt :col p min-n) — min-n
   ;; returns nil when fewer than min-n finite values remain (floor-free default).
   ;; p may be a vector of probabilities, sorting once and returning a vector.
   :qnt (fn ([col p] (if (sequential? p)
                       (math/quantiles-type7 col p)
                       (math/quantile-type7 col p)))
          ([col p min-n] (if (sequential? p)
                           (math/quantiles-type7 col p min-n)
                           (math/quantile-type7 col p min-n))))
   :sd dfn/standard-deviation
   :mx col-max
   :mi col-min
   :prod col-prod
   :variance dfn/variance
   :ct count-non-nil
   :in (fn [col s]
         (dtype/make-reader :boolean (dtype/ecount col)
                            (boolean (contains? s (nth col idx)))))
   :between? (fn [col lo hi] (dfn/and (gte-op col lo) (lte-op col hi)))
   :nuniq count-distinct
   :first-val first-val
   :last-val last-val
   :wavg wavg
   :wsum wsum})

(def ^:private sym->op
  "Maps source-form symbols to canonical keyword op names."
  {'+ :+, '- :-, '* :*, '/ :div
   'sq :sq, 'log :log, 'asinh :asinh
   'abs :abs, 'exp :exp, 'sqrt :sqrt, 'pow :pow
   'floor :floor, 'ceil :ceil, 'round :round, 'signum :signum
   'na2zero :na2zero, 'neg2na :neg2na, 'nonfin2na :nonfin2na
   ;; calendar date-parts, with a long alias for dow matching java.time naming
   'year :year, 'month :month, 'day :day, 'dow :dow, 'quarter :quarter
   'day-of-week :dow
   '> :>, '< :<, '>= :>=, '<= :<=, '= :=
   'and :and, 'or :or, 'not :not
   'mn :mn, 'sm :sm, 'md :md, 'sd :sd, 'mx :mx, 'mi :mi, 'qnt :qnt
   ;; full-name aliases for the aggregation ops (datajure.core names), so both
   ;; (mn :x) and (mean :x) work inside #dt/e
   'mean :mn, 'sum :sm, 'median :md, 'stddev :sd, 'variance :variance
   'max* :mx, 'min* :mi, 'count* :ct, 'ct :ct, 'prod :prod
   ;; concise aliases for the remaining aggregation ops, so the full datajure.concise
   ;; vocabulary works inside #dt/e (e.g. both (count-distinct :x) and (nuniq :x))
   'nuniq :nuniq, 'fst :first-val, 'lst :last-val, 'wa :wavg, 'ws :wsum
   'in :in, 'between? :between?, 'count-distinct :nuniq
   'first-val :first-val, 'last-val :last-val
   'wavg :wavg, 'wsum :wsum
   'div0 :div0
   'when-finite :when-finite
   ;; row count as a real nullary op — #dt/e (nrow) / (N) — so it composes in
   ;; arithmetic (e.g. (- (nrow) 1)) instead of needing the bare value marker
   'nrow :nrow, 'N :nrow})

(def ^:private expr-head-aliases
  "Aggregation aliases valid ONLY in expression-head position: (max :x) == (mx :x),
  (min :x) == (mi :x), (count :x) == (ct :x). Inside #dt/e symbols are a closed
  vocabulary (never resolved as vars), so the clojure.core-shadowing rationale for
  the star names does not apply — SQL's COUNT(x)/MAX(x) muscle memory does.

  Deliberately a SEPARATE table from sym->op: the win/scan & win/each-prior
  operator slots (and their kw->op-derived data-form spellings, via data-scan-op)
  rely on bare max/min falling through sym->op to stay the element-wise binary
  ops :max/:min. Head position and operator position are different grammar; keep
  the vocabularies separate rather than special-casing one shared table."
  {'max :mx, 'min :mi, 'count :ct})

(def ^:private expr-head-kw-aliases
  "Keyword spellings of expr-head-aliases for data-form heads ([:max :x] ==
  [:mx :x]) — consulted by data-op->kw only, never by data-scan-op, so
  [:win/scan :max …] keeps its operator meaning."
  (into {} (map (fn [[s k]] [(keyword (str s)) k])) expr-head-aliases))

(defn damerau-levenshtein
  "Damerau-Levenshtein edit distance: insertions, deletions, substitutions,
  and single adjacent transpositions each cost 1. Used by both the #dt/e
  op-name validator (suggest-op) and core's column-name validator
  (validate-expr-cols/validate-select-cols) to produce typo suggestions.

  Single source of truth — core.clj re-uses this via expr/damerau-levenshtein
  rather than maintaining a duplicate implementation."
  [s t]
  (let [s (vec s) t (vec t)
        m (count s) n (count t)
        d (make-array Long/TYPE (inc m) (inc n))]
    (dotimes [i (inc m)] (aset-long d i 0 i))
    (dotimes [j (inc n)] (aset-long d 0 j j))
    (dotimes [i m]
      (dotimes [j n]
        (let [i+ (inc i) j+ (inc j)
              cost (if (= (get s i) (get t j)) 0 1)
              del (inc (aget d i j+))
              ins (inc (aget d i+ j))
              sub (+ (aget d i j) cost)
              basic (min del ins sub)
              trans (if (and (>= i 1) (>= j 1)
                             (= (get s i) (get t (dec j)))
                             (= (get s (dec i)) (get t j)))
                      (+ (aget d (dec i) (dec j)) cost)
                      Long/MAX_VALUE)]
          (aset-long d i+ j+ (min basic trans)))))
    (aget d m n)))

(def ^:private known-ops
  "Union of all symbols parse-form recognizes: base ops, win/*, row/*, stat/*,
  plus structural special forms. Used for suggestion generation on typos."
  (delay
    (into (sorted-set)
          (concat (keys sym->op)
                  (keys expr-head-aliases)
                  (keys win-sym->op)
                  (keys row-sym->op)
                  (keys stat-sym->op)
                  '[if cond coalesce coalesce-finite let cut xbar win/scan win/each-prior]))))

(defn- suggest-op [op]
  (let [op-str (str op)
        candidates (->> @known-ops
                        (map (fn [k] [k (damerau-levenshtein op-str (str k))]))
                        (filter (fn [[k d]]
                                  (and (<= d 2)
                                       (>= (count (str k)) 2))))
                        (sort-by second)
                        (take 3)
                        (mapv first))]
    (seq candidates)))

(defn- ->op-kw
  "Normalise a source-form op to its canonical keyword.
  At read time, ops arrive as plain symbols ('and, '>, etc.)."
  [op]
  (or (sym->op op)
      (expr-head-aliases op)
      (when (keyword? op) op)
      (let [suggestions (when (symbol? op) (suggest-op op))]
        (throw (ex-info
                (str "Unknown op `" op "` in #dt/e expression."
                     (when suggestions
                       (str " Did you mean: "
                            (str/join ", " (map #(str "`" % "`") suggestions))
                            "?")))
                {:dt/error :unknown-op
                 :dt/op op
                 :dt/op-type (type op)
                 :dt/suggestions suggestions})))))

(defn- check-op-arity!
  "Read-time arity checks for ops with a friendly documented error. `wavg`/`wsum`
  (and their `wa`/`ws` aliases) take exactly two arguments; `and`/`or` are variadic
  but need at least one predicate (zero-arity is meaningless in a vectorized mask)."
  [op-kw op-sym n-args]
  (cond
    (and (#{:wavg :wsum} op-kw) (not= 2 n-args))
    (throw (ex-info
            (str "`" op-sym "` takes exactly two arguments (weight column, value column). Got "
                 n-args ".")
            {:dt/error :wrong-arity :dt/op op-sym :dt/expected 2 :dt/got n-args}))

    (and (#{:and :or} op-kw) (zero? n-args))
    (throw (ex-info
            (str "`" op-sym "` requires at least one argument in #dt/e. Got 0.")
            {:dt/error :wrong-arity :dt/op op-sym :dt/expected :at-least-1 :dt/got n-args}))

    (and (#{:year :month :day :dow :quarter} op-kw) (not= 1 n-args))
    (throw (ex-info
            (str "`" op-sym "` takes exactly one argument (a date column or expression). Got "
                 n-args ".")
            {:dt/error :wrong-arity :dt/op op-sym :dt/expected 1 :dt/got n-args}))

    (and (#{:sq :log :asinh :abs :exp :sqrt :floor :ceil :round :signum
            :na2zero :neg2na :nonfin2na} op-kw)
         (not= 1 n-args))
    (throw (ex-info
            (str "`" op-sym "` takes exactly one argument (a column or expression). Got "
                 n-args ".")
            {:dt/error :wrong-arity :dt/op op-sym :dt/expected 1 :dt/got n-args}))

    (and (= :pow op-kw) (not= 2 n-args))
    (throw (ex-info
            (str "`" op-sym "` takes exactly two arguments (base, exponent). Got "
                 n-args ".")
            {:dt/error :wrong-arity :dt/op op-sym :dt/expected 2 :dt/got n-args}))

    (and (#{:mn :sm :md :sd :variance :mx :mi :ct :nuniq :prod
            :first-val :last-val} op-kw)
         (not= 1 n-args))
    (throw (ex-info
            (str "`" op-sym "` is a column aggregation and takes exactly one argument. Got "
                 n-args "."
                 (when (#{:mx :mi} op-kw)
                   " For a row-wise max/min across columns, use `row/max`/`row/min`."))
            {:dt/error :wrong-arity :dt/op op-sym :dt/expected 1 :dt/got n-args}))

    (and (= :when-finite op-kw) (not= 2 n-args))
    (throw (ex-info
            (str "`" op-sym "` takes exactly two arguments (guard expression, body expression). Got "
                 n-args ".")
            {:dt/error :wrong-arity :dt/op op-sym :dt/expected 2 :dt/got n-args}))

    (and (= :nrow op-kw) (pos? n-args))
    (throw (ex-info
            (str "`" op-sym "` takes no arguments — it is the group/dataset row count. Got "
                 n-args ". For a non-nil count of a column use `count*`/`ct`.")
            {:dt/error :wrong-arity :dt/op op-sym :dt/expected 0 :dt/got n-args}))))

(defn- check-arith-nil-literal!
  "Arithmetic ops require non-nil operands. A literal nil — e.g. `(+ :x nil)` —
  is rejected at read time with a clear error: nil is ambiguous in arithmetic
  (is `(+ 3 nil)` 3? nil? NaN?), so the user declares intent explicitly via
  `coalesce` or `div0`. Predicates keep their unambiguous nil-literal → false
  rule, so this check is scoped to arithmetic only."
  [op-kw op-sym args]
  (when (and (#{:+ :- :* :div :sq :log
                :abs :exp :sqrt :pow :floor :ceil :round :signum} op-kw)
             (some nil? args))
    (throw (ex-info
            (str "Arithmetic op `" op-sym "` received a literal nil. Arithmetic "
                 "requires non-nil operands — use `coalesce` to supply a value, "
                 "or `div0` for nil-safe division.")
            {:dt/error :arith-nil-literal
             :dt/op op-sym}))))

;; ---------------------------------------------------------------------------
;; AST node constructors
;; ---------------------------------------------------------------------------

(defn col-node
  "Create a column reference AST node. `kw` is a keyword naming the column."
  [kw]
  {:node/type :col :col/name kw})

(defn expr-node?
  "Returns true if x is a #dt/e AST node (a map with :node/type)."
  [x]
  (and (map? x) (contains? x :node/type)))

(defn lit-node
  "Create a literal value AST node. `v` is any scalar (number, string, keyword, set, etc.)."
  [v]
  {:node/type :lit :lit/value v})

(defn op-node
  "Create an operation AST node. `op` is an op keyword (e.g. :+, :>), `args` is a seq of child AST nodes."
  [op args]
  {:node/type :op :op/name op :op/args args})

(defn win-node
  "AST node for a window function call.
  win-op is a keyword like :win/rank. args are parsed AST nodes."
  [win-op args]
  {:node/type :win :win/op win-op :win/args args})

(defn row-node
  "AST node for a row-wise function call.
  row-op is a keyword like :row/sum. args are parsed AST nodes."
  [row-op args]
  {:node/type :row :row/op row-op :row/args args})

(defn stat-node
  "AST node for a stat/* function call.
  stat-op is a keyword like :stat/standardize. args are parsed AST nodes."
  [stat-op args]
  {:node/type :stat :stat/op stat-op :stat/args args})

;; ---------------------------------------------------------------------------
;; AST builder: Clojure form -> AST
;; ---------------------------------------------------------------------------

(defn- parse-form
  ([form] (parse-form form #{}))
  ([form env]
   (cond
     (keyword? form) (col-node form)
     (and (symbol? form) (contains? env form)) {:node/type :binding-ref :binding-ref/name (keyword form)}
     (seq? form)
     (let [[op & args] form]
       (cond
         (= op 'if)
         (let [[pred then else] args]
           {:node/type :if
            :if/pred (parse-form pred env)
            :if/then (parse-form then env)
            :if/else (if (some? else) (parse-form else env) (lit-node nil))})
         (= op 'cond)
         (let [pairs (partition 2 args)]
           (reduce (fn [else-node [test then]]
                     {:node/type :if
                      :if/pred (if (= test :else) (lit-node true) (parse-form test env))
                      :if/then (parse-form then env)
                      :if/else else-node})
                   (lit-node nil)
                   (reverse pairs)))
         (or (= op 'coalesce) (= op 'coalesce-finite) (= op 'coalescef))
         {:node/type :coalesce
          :coalesce/args (mapv #(parse-form % env) args)
          :coalesce/finite? (not= op 'coalesce)}
         (= op 'let)
         (let [[binding-vec body] args
               binding-pairs (partition 2 binding-vec)
               [bindings new-env]
               (reduce (fn [[acc env] [sym expr]]
                         [(conj acc {:binding/name (keyword sym)
                                     :binding/expr (parse-form expr env)})
                          (conj env sym)])
                       [[] env]
                       binding-pairs)]
           {:node/type :let
            :let/bindings bindings
            :let/body (parse-form body new-env)})
         (= op 'cut)
         (let [[col-form n-form & rest-args] args
               from-expr (when (= (first rest-args) :from) (second rest-args))]
           {:node/type :cut
            :cut/col (parse-form col-form env)
            :cut/n (parse-form n-form env)
            :cut/from (when from-expr (parse-form from-expr env))})
         (= op 'xbar)
         (let [[col-form width-form unit-kw] args]
           {:node/type :xbar
            :xbar/col (parse-form col-form env)
            :xbar/width (parse-form width-form env)
            :xbar/unit unit-kw})
         (= op 'win/scan)
         (let [[scan-sym col-form] args
               ;; Mirror win/each-prior normalisation: prefer the canonical op
               ;; keyword from sym->op so scan ops are named consistently with
               ;; the rest of the codebase (e.g. '/' -> :div, not :/). Falls
               ;; back to (keyword (name ...)) for symbols without sym->op
               ;; entries (max/min) so the valid scan ops (+ * max min)
               ;; remain unchanged.
               scan-op-kw (or (sym->op scan-sym) (keyword (name scan-sym)))]
           {:node/type :scan
            :scan/op scan-op-kw
            :scan/arg (parse-form col-form env)})
         (= op 'win/each-prior)
         (let [[ep-sym col-form] args
               ep-op-kw (or (sym->op ep-sym) (keyword (name ep-sym)))]
           {:node/type :each-prior
            :each-prior/op ep-op-kw
            :each-prior/arg (parse-form col-form env)})
         (contains? win-sym->op op)
         (win-node (win-sym->op op) (mapv #(parse-form % env) args))
         (contains? row-sym->op op)
         (row-node (row-sym->op op) (mapv #(parse-form % env) args))
         (contains? stat-sym->op op)
         (stat-node (stat-sym->op op) (mapv #(parse-form % env) args))
         :else
         (let [op-kw (->op-kw op)]
           (check-op-arity! op-kw op (count args))
           (check-arith-nil-literal! op-kw op args)
           (op-node op-kw (mapv #(parse-form % env) args)))))
     :else (lit-node form))))

;; ---------------------------------------------------------------------------
;; Runtime data-form builder: evaluated Clojure data -> AST
;; ---------------------------------------------------------------------------
;;
;; A data-form is the runtime-data analogue of a #dt/e expression: plain
;; *evaluated* Clojure data, so a runtime value (a local, a parameter) flows
;; straight in where #dt/e — a read-time reader tag — can only see a literal.
;; It compiles down the exact same AST / vectorized path.

(def ^:private data-op-aliases
  "Keyword op aliases accepted in data-forms -> canonical op keyword. Only the
  ops whose natural keyword differs from the canonical name need an entry;
  everything else falls out of the sym->op table."
  {:/ :div
   :N :nrow})

(def ^:private kw->op
  "Keyword spelling of every base-op symbol data-forms accept — the full #dt/e
  vocabulary (canonical names plus every full-name/concise alias), derived from
  sym->op so the two spellings can never drift."
  (delay
    (into data-op-aliases
          (map (fn [[s k]] [(keyword (str s)) k]))
          sym->op)))

(def ^:private data-special-op?
  "Vector heads handled structurally by data->ast (mirroring parse-form's special
  forms) rather than through the op tables."
  #{:if :cond :let :coalesce :coalesce-finite :coalescef :cut :xbar
    :win/scan :win/each-prior})

(defn- data-op->kw
  "Normalise and validate a data-form op head (a keyword like :=, :>, :qnt,
  :win/lag). Returns the canonical op keyword, tagged with how it dispatches."
  [op]
  (if-not (keyword? op)
    (throw (ex-info (str "data-form op must be a keyword (e.g. :=, :>, :win/lag); got "
                         (pr-str op) ".")
                    {:dt/error :invalid-data-op :dt/op op}))
    (or (when-let [k (get @kw->op op)] [:op k])
        (when-let [k (get expr-head-kw-aliases op)] [:op k])
        (when (contains? win-op-table op) [:win op])
        (when (contains? row-op-table op) [:row op])
        (when (contains? stat-op-table op) [:stat op])
        (let [suggestions (suggest-op (symbol (str (when-let [ns (namespace op)] (str ns "/"))
                                                   (name op))))]
          (throw (ex-info (str "Unknown data-form op " op "."
                               (when suggestions
                                 (str " Did you mean: "
                                      (str/join ", " (map #(str "`:" % "`") suggestions))
                                      "?")))
                          {:dt/error :unknown-data-op
                           :dt/op op
                           :dt/suggestions suggestions}))))))

(defn- data-scan-op
  "Normalise the operator argument of a data-form [:win/scan op …] /
  [:win/each-prior op …] — a keyword like :+, :/, :max — to the canonical op
  keyword, mirroring parse-form's symbol normalisation."
  [op-kw form]
  (if (keyword? op-kw)
    (or (get @kw->op op-kw) op-kw)
    (throw (ex-info (str (first form) " in a data-form takes its operator as a keyword"
                         " (e.g. [:win/scan :* …]); got " (pr-str op-kw) ".")
                    {:dt/error :invalid-data-op :dt/op op-kw}))))

(defn data->ast
  "Convert a runtime data-form expression to a #dt/e AST so it compiles down the
  same vectorized path. The form mirrors #dt/e one-for-one, as plain *evaluated*
  data — one expression language, two spellings:
    - a vector [op-kw & args] is an operation. op-kw is the keyword spelling of
      any #dt/e op — element-wise (:>, :div0, :na2zero, …), aggregator (:mn,
      :qnt, :nrow, … including every full-name/concise alias), window/row/stat
      (:win/lag, :row/sum, :stat/winsorize, …);
    - the special forms are vector-headed too: [:if pred then else?],
      [:cond p1 v1 … :else d], [:let [:name expr …] body],
      [:coalesce …]/[:coalesce-finite …], [:cut :col n]/[:cut :col n :from pred],
      [:xbar :col w]/[:xbar :col w :minutes], [:win/scan :* expr],
      [:win/each-prior :- expr];
    - a **number-headed** vector is a literal value (e.g. a probability list
      [0.2 0.5 0.8] for a multi-quantile `qnt`);
    - a keyword is a column reference (or a [:let] binding reference in a body);
    - anything else (number, string, set, the value of a local) is a literal.
  So (data->ast [:= :tic ticker]) closes over the runtime value of `ticker`, and
  (data->ast [:win/lag :price 1]) is exactly #dt/e (win/lag :price 1). Context
  rules (e.g. win/* only in :set) are enforced by dt on the resulting AST, the
  same as for #dt/e. Use sets (not vectors) for `:in` membership, since a
  non-number-headed vector denotes an operation.

  The `ctx` argument of the 2-arity is retained for call compatibility and
  ignored — the op vocabulary is uniform across contexts, exactly like #dt/e."
  ([form] (data->ast form nil))
  ([form _ctx]
   (letfn [(go [f env]
             (cond
               (keyword? f) (if (contains? env f)
                              {:node/type :binding-ref :binding-ref/name f}
                              (col-node f))
               (and (vector? f) (number? (first f))) (lit-node f)
               (vector? f) (go-vec f env)
               :else (lit-node f)))
           (go-vec [f env]
             (let [[h & args] f]
               (if (data-special-op? h)
                 (case h
                   :if
                   (let [[pred then else] args]
                     {:node/type :if
                      :if/pred (go pred env)
                      :if/then (go then env)
                      :if/else (if (some? else) (go else env) (lit-node nil))})
                   :cond
                   (reduce (fn [else-node [test then]]
                             {:node/type :if
                              :if/pred (if (= test :else) (lit-node true) (go test env))
                              :if/then (go then env)
                              :if/else else-node})
                           (lit-node nil)
                           (reverse (partition 2 args)))
                   (:coalesce :coalesce-finite :coalescef)
                   {:node/type :coalesce
                    :coalesce/args (mapv #(go % env) args)
                    :coalesce/finite? (not= h :coalesce)}
                   :let
                   (let [[bvec body] args
                         _ (when-not (and (vector? bvec) (even? (count bvec))
                                          (every? keyword? (take-nth 2 bvec)))
                             (throw (ex-info (str "[:let …] bindings must be a vector of"
                                                  " keyword/expression pairs; got " (pr-str bvec) ".")
                                             {:dt/error :invalid-data-op :dt/op :let})))
                         [bindings env']
                         (reduce (fn [[acc e] [k bexpr]]
                                   [(conj acc {:binding/name k :binding/expr (go bexpr e)})
                                    (conj e k)])
                                 [[] env]
                                 (partition 2 bvec))]
                     {:node/type :let :let/bindings bindings :let/body (go body env')})
                   :cut
                   (let [[col-form n-form & rest-args] args
                         from-expr (when (= (first rest-args) :from) (second rest-args))]
                     {:node/type :cut
                      :cut/col (go col-form env)
                      :cut/n (go n-form env)
                      :cut/from (when from-expr (go from-expr env))})
                   :xbar
                   (let [[col-form width-form unit-kw] args]
                     {:node/type :xbar
                      :xbar/col (go col-form env)
                      :xbar/width (go width-form env)
                      :xbar/unit unit-kw})
                   :win/scan
                   (let [[scan-op col-form] args]
                     {:node/type :scan
                      :scan/op (data-scan-op scan-op f)
                      :scan/arg (go col-form env)})
                   :win/each-prior
                   (let [[ep-op col-form] args]
                     {:node/type :each-prior
                      :each-prior/op (data-scan-op ep-op f)
                      :each-prior/arg (go col-form env)}))
                 (let [[kind op-kw] (data-op->kw h)]
                   (case kind
                     :win (win-node op-kw (mapv #(go % env) args))
                     :row (row-node op-kw (mapv #(go % env) args))
                     :stat (stat-node op-kw (mapv #(go % env) args))
                     :op (do (check-op-arity! op-kw h (count args))
                             (check-arith-nil-literal! op-kw h args)
                             (op-node op-kw (mapv #(go % env) args))))))))]
     (go form #{}))))

;; ---------------------------------------------------------------------------
;; Compiler: AST -> fn of dataset
;; ---------------------------------------------------------------------------

(defn col-refs
  "Extract the set of column keywords referenced by an AST node.
  If a :lit node contains an embedded expr-node (from expression composition),
  recursively extracts its col-refs."
  [node]
  (case (:node/type node)
    :col #{(:col/name node)}
    :lit (let [v (:lit/value node)]
           (if (expr-node? v) (col-refs v) #{}))
    :binding-ref #{}
    :op (into #{} (mapcat col-refs) (:op/args node))
    :win (into #{} (mapcat col-refs) (:win/args node))
    :scan (col-refs (:scan/arg node))
    :each-prior (col-refs (:each-prior/arg node))
    :cut (let [base (into (col-refs (:cut/col node)) (col-refs (:cut/n node)))]
           (if-let [from (:cut/from node)]
             (into base (col-refs from))
             base))
    :xbar (into (col-refs (:xbar/col node)) (col-refs (:xbar/width node)))
    :row (into #{} (mapcat col-refs) (:row/args node))
    :stat (into #{} (mapcat col-refs) (:stat/args node))
    :if (into (col-refs (:if/pred node))
              (concat (col-refs (:if/then node))
                      (col-refs (:if/else node))))
    :coalesce (into #{} (mapcat col-refs) (:coalesce/args node))
    :let (into (into #{} (mapcat (comp col-refs :binding/expr)) (:let/bindings node))
               (col-refs (:let/body node)))))

(defn win-refs
  "Extract the set of window op keywords (e.g. :win/rank) referenced by an AST node.
  If a :lit node contains an embedded expr-node (from expression composition),
  recursively extracts its win-refs."
  [node]
  (case (:node/type node)
    :col #{}
    :lit (let [v (:lit/value node)]
           (if (expr-node? v) (win-refs v) #{}))
    :binding-ref #{}
    :op (into #{} (mapcat win-refs) (:op/args node))
    :win (into #{(:win/op node)} (mapcat win-refs) (:win/args node))
    :scan (conj (win-refs (:scan/arg node)) :win/scan)
    :each-prior (conj (win-refs (:each-prior/arg node)) :win/each-prior)
    :cut (let [base (into (win-refs (:cut/col node)) (win-refs (:cut/n node)))]
           (if-let [from (:cut/from node)]
             (into base (win-refs from))
             base))
    :xbar (into (win-refs (:xbar/col node)) (win-refs (:xbar/width node)))
    :row (into #{} (mapcat win-refs) (:row/args node))
    :stat (into #{} (mapcat win-refs) (:stat/args node))
    :if (into (win-refs (:if/pred node))
              (concat (win-refs (:if/then node))
                      (win-refs (:if/else node))))
    :coalesce (into #{} (mapcat win-refs) (:coalesce/args node))
    :let (into (into #{} (mapcat (comp win-refs :binding/expr)) (:let/bindings node))
               (win-refs (:let/body node)))))

(defn- xbar-bucket
  "Floor-divide a column by width to produce xbar bucket values.
  For numeric columns: quot(col, width) * width.
  For datetime columns: convert to epoch-milliseconds, divide by ms-per-unit,
  then floor-divide by width. Returns a reader of longs (nil preserved for missing).
  Uses (nil? (nth rdr idx)) for nil detection, which works for both full Column
  objects and plain dtype readers."
  [col width unit]
  (let [col-dtype (-> col meta :datatype)
        rdr (dtype/->reader col)
        n (dtype/ecount rdr)]
    (if (and col-dtype (dtype-dt/datetime-datatype? col-dtype))
      (let [ms-per-unit (or (some->> unit (math/canonical-unit math/ms-per-unit) math/ms-per-unit)
                            (throw (ex-info (str "Unknown xbar temporal unit: " unit
                                                 ". Supported: :seconds :minutes :hours :days :weeks"
                                                 " (singular spellings accepted).")
                                            {:dt/error :xbar-unknown-unit :unit unit})))]
        (dtype/make-reader :object n
                           (let [v (nth rdr idx)]
                             (if (nil? v)
                               nil
                               (let [epoch-ms (dtype-dt/datetime->epoch :epoch-milliseconds v)
                                     epoch-units (Math/floorDiv ^long epoch-ms ^long ms-per-unit)]
                                 (* width (Math/floorDiv ^long epoch-units ^long width)))))))
      (dtype/make-reader :object n
                         (let [v (nth rdr idx)]
                           (if (nil? v)
                             nil
                             (* width (Math/floorDiv (long v) (long width)))))))))

(defn- cut-bucket
  "Assign each element of col to an equal-count bin in 1..n.
  Breakpoints are computed from the reference population:
  - mask nil: all non-nil values of col (plain cut).
  - mask a boolean reader: col values where mask is true and col is non-nil.
    This is the NYSE-style use case: mask selects a reference subpopulation
    (e.g. NYSE stocks only); breakpoints are computed from that subset and
    applied to all rows of col.
  Bin assignment uses right-open intervals (Java binarySearch). nil values in
  col produce nil. All non-nil values land in [1, n].
  Uses (nil? (nth rdr idx)) for nil detection, which works for both full Column
  objects and plain dtype readers."
  ([col n] (cut-bucket col n nil))
  ([col n mask]
   (let [n (int n)
         rdr (dtype/->reader col)
         cnt (dtype/ecount rdr)
         ref-pop (if (some? mask)
                   (filterv some?
                            (map-indexed (fn [i v] (when (nth mask i) v)) rdr))
                   (filterv some? rdr))
         ;; type-7 breakpoints at the 1/n, 2/n, ..., (n-1)/n quantiles (R default)
         breaks-arr (double-array
                     (map #(or (math/quantile-type7 ref-pop (/ (double %) n)) ##NaN)
                          (range 1 n)))]
     (dtype/make-reader :object cnt
                        (let [v (nth rdr idx)]
                          (if (nil? v)
                            nil
                            (let [dv (double v)
                                  r (java.util.Arrays/binarySearch breaks-arr dv)
                                  b (if (>= r 0) r (- (- r) 1))]
                              (min (inc b) n))))))))

(defn compile-expr
  "Compile an AST node to a fn [ds] -> column/scalar.
  Column keywords resolve to dataset columns; literals pass through;
  ops dispatch via op-table to dfn functions.

  Nil-safety: if any arg to an op evaluates to nil (e.g. a nil literal),
  comparison ops return an all-false boolean column; arithmetic ops return nil
  (which becomes a missing value when stored in a dataset column)."
  ([node] (compile-expr node {}))
  ([node env]
   (case (:node/type node)
     :col (fn [ds] (ds (:col/name node)))
     :lit (let [v (:lit/value node)]
            (if (expr-node? v)
              (compile-expr v env)
              (fn [_ds] v)))
     :binding-ref (let [kw (:binding-ref/name node)
                        bound-fn (get env kw)]
                    (fn [ds] (bound-fn ds)))
     :if (let [pred-fn (compile-expr (:if/pred node) env)
               then-fn (compile-expr (:if/then node) env)
               else-fn (compile-expr (:if/else node) env)]
           (fn [ds]
             (let [pred (pred-fn ds)
                   then (then-fn ds)
                   else (else-fn ds)
                   n (ds/row-count ds)]
               (cond
                 (nil? pred) else
                 (true? pred) then
                 (false? pred) else
                 :else
                 (dtype/make-reader :object n
                                    (let [t (if (dtype/reader? then) (nth then idx) then)
                                          e (if (dtype/reader? else) (nth else idx) else)]
                                      (if (nth pred idx) t e)))))))
     :coalesce (let [arg-fns (mapv #(compile-expr % env) (:coalesce/args node))
                     ;; coalesce → first non-nil; coalesce-finite → first finite
                     ;; (skips nil AND NaN/±Inf; number? guards finite-double? which
                     ;; would throw on a non-numeric fallback)
                     present? (if (:coalesce/finite? node)
                                (fn [v] (and (number? v) (math/finite-double? v)))
                                some?)]
                 (fn [ds]
                   (let [n (ds/row-count ds)
                         cols (mapv #(% ds) arg-fns)]
                     (dtype/make-reader :object n
                                        (let [vals (map #(if (dtype/reader? %) (nth % idx) %) cols)]
                                          (first (filter present? vals)))))))
     :let (let [final-env
                (reduce (fn [acc-env b]
                          (let [kw (:binding/name b)
                                compiled-fn (compile-expr (:binding/expr b) acc-env)]
                            (assoc acc-env kw compiled-fn)))
                        env
                        (:let/bindings node))
                body-fn (compile-expr (:let/body node) final-env)]
            (fn [ds] (body-fn ds)))
     :scan (let [op-kw (:scan/op node)
                 arg-fn (compile-expr (:scan/arg node) env)]
             (fn [ds]
               (win/win-scan op-kw (arg-fn ds))))
     :each-prior (let [op-kw (:each-prior/op node)
                       arg-fn (compile-expr (:each-prior/arg node) env)]
                   (fn [ds]
                     (win/win-each-prior op-kw (arg-fn ds))))
     :cut (let [col-fn (compile-expr (:cut/col node) env)
                n-fn (compile-expr (:cut/n node) env)
                mask-fn (when-let [from (:cut/from node)]
                          (compile-expr from env))]
            (fn [ds]
              (cut-bucket (col-fn ds) (n-fn ds) (when mask-fn (mask-fn ds)))))
     :xbar (let [col-fn (compile-expr (:xbar/col node) env)
                 width-fn (compile-expr (:xbar/width node) env)
                 unit (:xbar/unit node)]
             (fn [ds]
               (xbar-bucket (col-fn ds) (width-fn ds) unit)))
     :win (let [win-op-kw (:win/op node)
                win-fn (or (win-op-table win-op-kw)
                           (throw (ex-info (str "Unknown window op: " win-op-kw)
                                           {:dt/error :unknown-win-op :win-op win-op-kw})))
                arg-fns (mapv #(compile-expr % env) (:win/args node))]
            (fn [ds]
              (let [args (map #(% ds) arg-fns)]
                (apply win-fn args))))
     :row (let [row-op-kw (:row/op node)
                row-fn (or (row-op-table row-op-kw)
                           (throw (ex-info (str "Unknown row op: " row-op-kw)
                                           {:dt/error :unknown-row-op :row-op row-op-kw})))
                arg-fns (mapv #(compile-expr % env) (:row/args node))]
            (fn [ds]
              (let [args (map #(% ds) arg-fns)]
                (apply row-fn args))))
     :stat (let [stat-op-kw (:stat/op node)
                 stat-fn (or (stat-op-table stat-op-kw)
                             (throw (ex-info (str "Unknown stat op: " stat-op-kw)
                                             {:dt/error :unknown-stat-op :stat-op stat-op-kw})))
                 arg-fns (mapv #(compile-expr % env) (:stat/args node))]
             (fn [ds]
               (let [args (map #(% ds) arg-fns)]
                 (apply stat-fn args))))
     :op (if (= :nrow (:op/name node))
           ;; row count needs the dataset itself, not evaluated column args —
           ;; the only op with that shape, so it short-circuits the op-table
           (fn [ds] (ds/row-count ds))
           (let [op-kw (:op/name node)
                 op-fn (or (op-table op-kw)
                           (throw (ex-info "Unknown op in #dt/e expression"
                                           {:op op-kw})))
                 arg-fns (mapv #(compile-expr % env) (:op/args node))
                 cmp? (comparison-ops op-kw)]
             (fn [ds]
               (let [args (map #(% ds) arg-fns)]
                 (if (some nil? args)
                   (if cmp?
                     (dtype/make-reader :boolean (ds/row-count ds) false)
                     nil)
                   (apply op-fn args)))))))))

;; ---------------------------------------------------------------------------
;; Reader tag handler
;; ---------------------------------------------------------------------------

(defn read-expr
  "Reader tag handler for #dt/e. Returns an AST map. Called at read time."
  [form]
  (parse-form form))

(defn register-reader!
  "Register the #dt/e reader tag via alter-var-root on *data-readers*.
  Fallback for AOT compilation or scripts where data_readers.clj is not picked
  up at startup. The primary registration mechanism is resources/data_readers.clj,
  which Clojure merges automatically for all threads at JVM startup."
  []
  (alter-var-root #'*data-readers* assoc 'dt/e #'read-expr))
