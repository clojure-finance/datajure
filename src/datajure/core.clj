(ns datajure.core
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.set :as set]
            [datajure.expr :as expr]
            [datajure.math :as math])
  (:import [org.roaringbitmap RoaringBitmap]))

(declare apply-order-by order-perm validate-select-cols info-note)

(defn- expr-node? [x]
  (and (map? x) (contains? x :node/type)))

(defn- validate-expr-cols
  "Pre-execution column validation for #dt/e AST nodes.
  Extracts column refs from the AST and checks against the dataset.
  Throws ex-info with helpful message if unknown columns found."
  [dataset node context]
  (let [refs (expr/col-refs node)
        available (set (ds/column-names dataset))
        unknown (clojure.set/difference refs available)]
    (when (seq unknown)
      (let [avail-names (vec (sort available))
            suggestions (into {}
                              (map (fn [col]
                                     (let [col-str (name col)
                                           closest (->> avail-names
                                                        (map (fn [a] [a (expr/damerau-levenshtein col-str (name a))]))
                                                        (sort-by second)
                                                        first)]
                                       [col (when (and closest (<= (second closest) 3)) [(first closest)])])))
                              unknown)]
        (throw (ex-info (str "Unknown column(s) " unknown " in " context " expression")
                        {:dt/error :unknown-column
                         :dt/columns unknown
                         :dt/context context
                         :dt/available avail-names
                         :dt/closest suggestions}))))))

(defn- validate-no-win
  "Checks that an #dt/e AST does not contain win/* references.
  Throws a structured error if window functions are found outside :set context."
  [node context]
  (let [wrefs (expr/win-refs node)]
    (when (seq wrefs)
      (throw (ex-info (str "Window function(s) " wrefs " in " context
                           " require :set context. Use :set to derive window columns.")
                      {:dt/error :win-outside-window
                       :dt/win-ops wrefs
                       :dt/context context})))))

(defn- validate-win-in-derivations
  "Scans a :set/:agg derivation map or vector-of-pairs for win/* outside window mode."
  [derivations context]
  (let [pairs (if (map? derivations) (seq derivations) derivations)]
    (doseq [[_col-kw col-fn] pairs]
      (when (expr-node? col-fn)
        (validate-no-win col-fn context)))))

(defn- derivations-have-win?
  "Returns true if any #dt/e expression in the derivations contains win/* references."
  [derivations]
  (let [pairs (if (map? derivations) (seq derivations) derivations)]
    (some (fn [[_col-kw col-fn]]
            (and (expr-node? col-fn) (seq (expr/win-refs col-fn))))
          pairs)))

(defn- validate-map-set-cross-refs
  "For map-form :set, detect expressions that reference sibling columns being derived.
  Map semantics are simultaneous — cross-references silently see original column values,
  which is almost certainly a mistake. Suggests vector-of-pairs for sequential semantics."
  [derivations]
  (when (map? derivations)
    (let [derived-cols (set (keys derivations))]
      (doseq [[col-kw col-val] derivations]
        (when (expr-node? col-val)
          (let [refs (expr/col-refs col-val)
                sibling-refs (clojure.set/intersection refs (disj derived-cols col-kw))]
            (when (seq sibling-refs)
              (throw (ex-info
                      (str "In map-form :set, column " col-kw
                           " references " sibling-refs
                           ", which are being derived in the same map."
                           " Map semantics are simultaneous — use vector-of-pairs"
                           " [[:col1 expr1] [:col2 expr2]] for sequential derivation.")
                      {:dt/error :map-set-cross-reference
                       :dt/column col-kw
                       :dt/sibling-refs sibling-refs
                       :dt/derived-cols derived-cols})))))))))

(defn- normalise-expr-value
  "Convert a data-form vector to its AST at the dt boundary, so data-forms and
  #dt/e expressions flow through identical validation and dispatch downstream —
  window-mode detection, win-outside-:set checks, map-:set cross-reference
  checks, and the fast-path introspection all see one representation."
  [v]
  (if (vector? v) (expr/data->ast v) v))

(defn- normalise-derivations
  "Normalise every value of a :set/:agg map or seq-of-pairs via
  normalise-expr-value, preserving the container shape (map stays a map —
  simultaneous semantics; a seq of pairs stays sequential)."
  [derivations]
  (cond
    (nil? derivations) nil
    (map? derivations) (update-vals derivations normalise-expr-value)
    :else (mapv (fn [[k v]] [k (normalise-expr-value v)]) derivations)))

(defn- normalise-order-spec
  "Normalise a sort spec to {:order :asc|:desc :col <kw>}. Accepts a bare column
  keyword (ascending), the (asc :col)/(desc :col) helper maps, and the data-form
  spelling [:asc :col]/[:desc :col] — the EDN-friendly variant for
  programmatically built queries. Anything else passes through for
  validate-order-specs to reject with a structured error."
  [s]
  (cond
    (keyword? s) {:order :asc :col s}
    (and (vector? s) (= 2 (count s)) (#{:asc :desc} (first s)))
    {:order (first s) :col (second s)}
    :else s))

(defn- order-spec-col
  "The column keyword a sort spec refers to, accepting all spellings."
  [s]
  (:col (normalise-order-spec s)))

(defn- apply-where [dataset predicate]
  (let [node (cond
               (expr-node? predicate) predicate
               ;; a runtime data-form vector (e.g. [:= :tic ticker]) desugars to
               ;; the same AST #dt/e produces, riding the vectorized path
               (vector? predicate) (expr/data->ast predicate)
               :else nil)]
    (if node
      (do (validate-expr-cols dataset node :where)
          (ds/select-rows dataset ((expr/compile-expr node) dataset)))
      (ds/filter dataset predicate))))

(defn- derive-column [dataset col-kw col-fn]
  (cond
    (expr-node? col-fn)
    (do (validate-expr-cols dataset col-fn (str ":set " col-kw))
        ((expr/compile-expr col-fn) dataset))
    ;; a runtime data-form vector (e.g. [:div0 [:- :a :b] :a] or [:qnt :x 0.2])
    ;; desugars to the same AST #dt/e compiles — see apply-where
    (vector? col-fn)
    (let [node (expr/data->ast col-fn :agg)]
      (validate-expr-cols dataset node (str ":set " col-kw))
      ((expr/compile-expr node) dataset))
    :else (mapv col-fn (ds/mapseq-reader dataset))))

(defn- apply-set [dataset derivations]
  (if (map? derivations)
    (reduce (fn [ds* [col-kw col-val]]
              (assoc ds* col-kw col-val))
            dataset
            (into {} (map (fn [[col-kw col-fn]]
                            [col-kw (derive-column dataset col-kw col-fn)])
                          derivations)))
    (reduce (fn [ds* [col-kw col-fn]]
              (assoc ds* col-kw (derive-column ds* col-kw col-fn)))
            dataset
            derivations)))

(defn- agg-result-footgun?
  "Detects the plain-fn :agg footgun where the user returned a column or dataset
  instead of a scalar. In :agg, a plain fn receives the group dataset, so
  `(:mass %)` returns a column vector, not a scalar — a common mistake for users
  coming from :set context where `(:mass %)` returns a scalar per row."
  [v]
  (or (ds/dataset? v)
      (instance? tech.v3.dataset.impl.column.Column v)))

(defn- eval-agg [dataset col-kw agg-fn]
  (cond
    (expr-node? agg-fn)
    (do (validate-expr-cols dataset agg-fn (str ":agg " col-kw))
        ((expr/compile-expr agg-fn) dataset))
    ;; runtime data-form vector, e.g. [:qnt :saleq 0.2] — see apply-where/derive-column
    (vector? agg-fn)
    (let [node (expr/data->ast agg-fn :agg)]
      (validate-expr-cols dataset node (str ":agg " col-kw))
      ((expr/compile-expr node) dataset))
    :else
    (let [result (agg-fn dataset)]
      (when (agg-result-footgun? result)
        (throw (ex-info
                (str ":agg plain function for column " col-kw
                     " returned a " (if (ds/dataset? result) "dataset" "column")
                     ", not a scalar. In :agg, plain functions receive the group"
                     " dataset, so `(:col %)` returns a column vector, not a scalar."
                     " Use `(dfn/mean (:col %))` for aggregation, or prefer"
                     " `#dt/e (mn :col)` which handles both cases uniformly.")
                {:dt/error :agg-plain-fn-returned-non-scalar
                 :dt/column col-kw
                 :dt/returned-type (if (ds/dataset? result) :dataset :column)})))
      result)))

(defn- apply-agg
  ([dataset aggregations] (apply-agg dataset aggregations nil))
  ([dataset aggregations within-order]
   (let [sorted (if within-order (apply-order-by dataset within-order :within-order) dataset)
         pairs (if (map? aggregations) (seq aggregations) aggregations)
         result (reduce (fn [m [col-kw agg-fn]]
                          (assoc m col-kw [(eval-agg sorted col-kw agg-fn)]))
                        {}
                        pairs)]
     (ds/->dataset result))))

(defn- percentile-breakpoints
  "Compute n-1 breakpoints at the 1/n, 2/n, ..., (n-1)/n quantiles of the
  non-nil values in col. Returns a vector of breakpoints, or nil if there are
  fewer than n non-nil values. Uses R type-7 quantiles (matching cut-bucket)."
  [col n]
  (let [finite (->> col dtype/->reader (remove nil?))
        k (count finite)]
    (when (>= k n)
      (mapv #(math/quantile-type7 finite (/ (double %) n)) (range 1 n)))))

(defn- bin-via-breakpoints
  "Given a scalar v and n-1 breakpoints (assumed sorted ascending), return the
  1-based bin index in [1, n]. Uses left-inclusive comparison: v lands in bin
  i (1-based) if v is <= breakpoints[i-1]. This matches cut-bucket's
  java.util.Arrays/binarySearch exact-match behaviour (exact hits return the
  lower bin), so cut-in-:by and cut-in-#dt/e produce identical bins for values
  equal to a breakpoint. Returns nil for nil input."
  [v breakpoints]
  (when (some? v)
    (loop [i 0]
      (cond
        (>= i (count breakpoints)) (inc (count breakpoints))
        (<= v (nth breakpoints i)) (inc i)
        :else (recur (inc i))))))

(defn- resolve-cut-marker
  "Given a {:dt/selector :cut ...} marker (from the standalone `cut` fn in :by)
  and the dataset, compute breakpoints once and return a metadata-tagged row-fn
  that bins each row's value. The returned fn carries :datajure/col metadata so
  the resulting group-key column has a friendly name.

  When :dt/from is present (a #dt/e expr-node or boolean column keyword),
  breakpoints are computed from the reference subpopulation where the mask is
  true and col is non-nil — mirroring the :from semantics of cut-bucket."
  [dataset marker]
  (let [col-kw (:dt/col marker)
        n (:dt/n marker)
        from (:dt/from marker)
        result-col (or (:datajure/col marker)
                       (keyword (str (name col-kw) "-q" n)))]
    (when-not (contains? (set (ds/column-names dataset)) col-kw)
      (throw (ex-info (str "cut: column " col-kw " not found in dataset")
                      {:dt/error :unknown-column
                       :dt/columns #{col-kw}
                       :dt/available (vec (sort (ds/column-names dataset)))})))
    (let [col (ds/column dataset col-kw)
          from-mask (when (some? from)
                      (dtype/->reader
                       (cond
                         (expr/expr-node? from) ((expr/compile-expr from) dataset)
                         (keyword? from) (ds/column dataset from)
                         :else (throw (ex-info "cut :from must be a #dt/e expression or column keyword"
                                               {:dt/error :cut-invalid-from :from from})))))
          ref-col (if (some? from-mask)
                    (let [rdr (dtype/->reader col)]
                      (filterv some?
                               (map-indexed (fn [i v] (when (nth from-mask i) v)) rdr)))
                    col)
          breakpoints (percentile-breakpoints ref-col n)]
      (with-meta
        (fn [row]
          (bin-via-breakpoints (get row col-kw) breakpoints))
        {:datajure/col result-col}))))

(defn- cut-marker? [x]
  (and (map? x) (= :cut (:dt/selector x))))

(defn- by->group-fn
  "Produce a row-to-group-key function from a :by spec. The dataset is required
  so that markers like cut (which need population-level statistics) can
  precompute their breakpoints once before grouping."
  [dataset by]
  (cond
    (fn? by)
    by
    (every? keyword? by)
    (fn [row] (select-keys row by))
    :else
    (let [resolved (mapv (fn [item]
                           (if (cut-marker? item)
                             (resolve-cut-marker dataset item)
                             item))
                         by)]
      (fn [row]
        (into {}
              (map-indexed (fn [i item]
                             (if (keyword? item)
                               [item (get row item)]
                               (let [col-name (or (-> item meta :xbar/col)
                                                  (-> item meta :datajure/col)
                                                  (keyword (str "fn-" i)))]
                                 [col-name (item row)])))
                           resolved))))))

(defn- validate-by-entries
  "Reject #dt/e ASTs and data-form vectors inside a sequential :by with a
  structured error. Without this, an AST map lands in by->group-fn's fn branch,
  where (ast-map row) is a lookup miss -> every row keys to nil -> the whole
  dataset silently collapses into ONE group. Derive the key with :set first."
  [by]
  (when (sequential? by)
    (doseq [item by]
      (when (or (expr-node? item) (vector? item))
        (throw (ex-info
                (str "Expressions aren't supported directly in :by (got "
                     (if (expr-node? item) "a #dt/e expression" (pr-str item))
                     "). Derive the group key with :set first, e.g. "
                     "(-> ds (dt :set {:m #dt/e (month :date)}) (dt :by [:m] :agg {...})).")
                {:dt/error :expr-in-by
                 :dt/entry item}))))))

(defn- needs-per-partition-resolution?
  "True if :by mixes tagged markers (currently cut) with exact keys — in
  which case markers must be resolved against each exact-key partition
  separately so breakpoints are per-group. Pure-marker :by (no exact keys)
  stays global because there is nothing to partition by. Pure-exact-key
  :by has no markers to resolve. Pure-fn :by is a user-controlled grouping
  and opts out of the marker machinery entirely."
  [by]
  (and (sequential? by)
       (some cut-marker? by)
       (some keyword? by)))

(defn- keyword-only-by?
  "True for a :by that is a non-empty sequence of plain column keywords — no
  cut/xbar markers and no fn. The fast group-agg path handles exactly this
  (the common Fama-French / peer-bands shape); marker/fn :by uses the general path."
  [by]
  (and (sequential? by) (seq by) (every? keyword? by)))

(defn- agg-col-refs
  "Columns an agg value references, or :all when it can't be introspected (a plain
  fn may touch any column). #dt/e and data-form aggs expose their refs via the AST."
  [agg-fn]
  (cond
    (expr-node? agg-fn) (expr/col-refs agg-fn)
    (vector? agg-fn) (expr/col-refs (expr/data->ast agg-fn :agg))
    :else :all))

(defn- narrow-for-aggs
  "Project `dataset` down to just the columns the group-agg actually touches —
  the :by keys, the columns the aggs reference, and any within-order sort columns
  — so the per-group `ds/select-rows` views (and their column wrappers) cover a
  handful of columns instead of all of them. Returns `dataset` unchanged if any
  agg is a plain fn (which could read any column)."
  [dataset by pairs within-order]
  (let [refs (map (comp agg-col-refs second) pairs)]
    (if (some #{:all} refs)
      dataset
      (let [order-cols (when within-order
                         (map order-spec-col within-order))
            needed (into (set by) (concat (apply concat refs) order-cols))]
        (ds/select-columns dataset (filterv needed (ds/column-names dataset)))))))

(def ^:private prim-quantile-ops
  "Aggregator ops eligible for the primitive double[] gather. Limited to the
  quantile ops because they drop non-finite values, so a gathered double[] with
  NaN-for-missing is exactly correct (unlike mn/sd, where missing != NaN)."
  #{:qnt :md})

(defn- prim-quantile-spec
  "When `agg-fn` is a single-column `qnt`/`md` op over a numeric, non-temporal
  column (literal prob/min-n args) and there is no `within-order` sort, return a
  spec `{:col :ps :min-n}` for the allocation-light primitive gather. Otherwise nil
  (the agg goes through the general per-group `eval-agg` path — which also yields
  the structured `:quantile-non-numeric` error for a temporal column)."
  [dataset agg-fn within-order]
  (when (nil? within-order)
    (let [node (cond (expr-node? agg-fn) agg-fn
                     (vector? agg-fn) (try (expr/data->ast agg-fn :agg) (catch Throwable _ nil))
                     :else nil)]
      (when (and node (= :op (:node/type node)) (prim-quantile-ops (:op/name node)))
        (let [args (:op/args node)
              c (first args)]
          (when (and c (= :col (:node/type c))
                     (every? #(= :lit (:node/type %)) (rest args)))
            (let [col-kw (:col/name c)
                  dt (some-> (ds/column dataset col-kw) meta :datatype)]
              (when (and dt (casting/numeric-type? dt) (not (dtype-dt/datetime-datatype? dt)))
                (if (= :md (:op/name node))
                  {:col col-kw :ps 0.5 :min-n nil}
                  (let [[p min-n] (mapv :lit/value (rest args))]
                    {:col col-kw :ps p :min-n min-n}))))))))))

(defn- fast-group-agg
  "Fast path for keyword-only :by. Narrows the dataset to the columns the agg
  actually touches, groups row indices over the key-column readers in a single
  pass (no per-row maps, no eager per-group split of every column), and assembles
  the result columns once — no per-group result datasets, no `ds/concat`.

  Single-column quantile aggregators (`qnt`/`md`) take a primitive path: each
  referenced column is pulled once as a `double[]`, and per group its slice is
  gathered by row index and reduced with no boxing. Every other agg (composite
  #dt/e, plain fn, order-sensitive) is fed a lazy `ds/select-rows` view through
  `eval-agg`. Same result as the general path: key columns first (in :by order),
  then agg columns (in :agg order); one row per group in first-seen order."
  [dataset0 by pairs within-order]
  (let [dataset (narrow-for-aggs dataset0 by pairs within-order)
        key-rdrs (mapv #(dtype/->reader (ds/column dataset %)) by)
        nk (count by)
        np (count pairs)
        n (ds/row-count dataset)
        ;; per-agg: a prim-quantile spec map, or :general
        specs (mapv (fn [pr] (or (prim-quantile-spec dataset (second pr) within-order) :general)) pairs)
        prim-cols (into #{} (comp (filter map?) (map :col)) specs)
        darrs (persistent! (reduce (fn [m c] (assoc! m c (dtype/->double-array (ds/column dataset c))))
                                   (transient {}) prim-cols))
        need-sub? (boolean (some #{:general} specs))
        groups (java.util.LinkedHashMap.)]
    (dotimes [i n]
      (let [k (mapv #(nth % i) key-rdrs)
            ^java.util.ArrayList lst (or (.get groups k)
                                         (let [a (java.util.ArrayList.)] (.put groups k a) a))]
        (.add lst (int i))))
    (let [g (.size groups)
          key-cols (vec (repeatedly nk #(object-array g)))
          agg-cols (vec (repeatedly np #(object-array g)))
          gi (int-array 1)]
      (doseq [^java.util.Map$Entry e (.entrySet groups)]
        (let [row (aget gi 0)
              ktuple (.getKey e)
              ^java.util.ArrayList idxs (.getValue e)
              m (.size idxs)
              idx-arr (int-array m)
              _ (dotimes [j m] (aset idx-arr j (int (.get idxs j))))
              sub (when need-sub?
                    (let [s0 (ds/select-rows dataset idx-arr)]
                      (if within-order (apply-order-by s0 within-order :within-order) s0)))
              ;; one scratch buffer per group, re-gathered per prim agg
              ^doubles buf (when (pos? (count darrs)) (double-array m))]
          (dotimes [c nk] (aset ^objects (nth key-cols c) row (nth ktuple c)))
          (dotimes [a np]
            (let [spec (nth specs a)
                  v (if (map? spec)
                      (let [^doubles darr (darrs (:col spec))]
                        (dotimes [j m] (aset buf j (aget darr (aget idx-arr j))))
                        (math/quantiles-of-doubles buf (:ps spec) (:min-n spec)))
                      (let [pr (nth pairs a)]
                        (eval-agg sub (first pr) (second pr))))]
              (aset ^objects (nth agg-cols a) row v)))
          (aset gi 0 (inc row))))
      (reduce (fn [d [cname cdata]] (ds/add-column d (ds/new-column cname cdata)))
              (ds/->dataset {})
              (concat (map vector by key-cols)
                      (map vector (map first pairs) agg-cols))))))

(defn- apply-group-agg
  ([dataset by aggregations] (apply-group-agg dataset by aggregations nil))
  ([dataset by aggregations within-order]
   (if (zero? (ds/row-count dataset))
     dataset
     (let [pairs (if (map? aggregations) (seq aggregations) aggregations)]
       (if (keyword-only-by? by)
         ;; Fast path: plain keyword :by (no markers/fn) — manual row-index group
         ;; + assemble-once, avoiding ds/group-by's eager all-column split.
         (fast-group-agg dataset by (vec pairs) within-order)
         ;; General path. Compound case (cut marker + exact keys): first partition
         ;; by exact keys so cut breakpoints are computed per sub-dataset.
         (let [partitions (if (needs-per-partition-resolution? by)
                            (let [exact-keys (filterv keyword? by)]
                              (vals (ds/group-by dataset (fn [row] (select-keys row exact-keys)))))
                            [dataset])]
           (->> partitions
                (mapcat (fn [partition-ds]
                          (let [group-fn (by->group-fn partition-ds by)
                                groups (ds/group-by partition-ds group-fn)]
                            (map (fn [[group-key sub-ds]]
                                   (let [sorted (if within-order (apply-order-by sub-ds within-order :within-order) sub-ds)
                                         wrapped-key (update-vals group-key vector)
                                         agg-result (reduce (fn [m [col-kw agg-fn]]
                                                              (assoc m col-kw [(eval-agg sorted col-kw agg-fn)]))
                                                            wrapped-key
                                                            pairs)]
                                     (ds/->dataset agg-result)))
                                 groups))))
                (apply ds/concat))))))))

(defn- deriv-ast
  "AST for a :set derivation value (for col/win-ref introspection), or nil for a
  plain fn (which can't be introspected → use the general path)."
  [dval]
  (cond
    (expr-node? dval) dval
    (vector? dval) (try (expr/data->ast dval :agg) (catch Throwable _ nil))
    :else nil))

(defn- group-perm
  "For keyword-only `by`, return {:perm int[] :bounds [[start end]…]}: row indices
  grouped by key (first-seen order) and, within each group, sorted by `within-order`
  (data order if nil), laid out contiguously. `bounds` gives each group's [start end)
  range in `perm` — the same grouped+sorted row order the general path produces."
  [dataset by within-order]
  (let [key-rdrs (mapv #(dtype/->reader (ds/column dataset %)) by)
        n (ds/row-count dataset)
        groups (java.util.LinkedHashMap.)]
    (dotimes [i n]
      (let [k (mapv #(nth % i) key-rdrs)]
        (.add ^java.util.ArrayList
         (or (.get groups k)
             (let [a (java.util.ArrayList.)] (.put groups k a) a))
              (int i))))
    (let [cmp (when within-order
                (let [specs (mapv normalise-order-spec within-order)
                      rdrs (mapv #(dtype/->reader (ds/column dataset (:col %))) specs)
                      dirs (int-array (map #(if (= :desc (:order %)) -1 1) specs))
                      ns (count specs)]
                  ;; a 3-way (-1/0/1) fn is directly usable as a java.util.Comparator
                  (fn [a b]
                    (loop [k 0]
                      (if (< k ns)
                        (let [c (compare (nth (nth rdrs k) a) (nth (nth rdrs k) b))]
                          (if (zero? c) (recur (inc k)) (* c (aget dirs k))))
                        0)))))
          perm (int-array n)
          bounds (java.util.ArrayList.)
          pos (int-array 1)]
      (doseq [^java.util.ArrayList idxs (.values groups)]
        (when cmp (java.util.Collections/sort idxs cmp))
        (let [start (aget pos 0) m (.size idxs)]
          (dotimes [j m] (aset perm (+ start j) (int (.get idxs j))))
          (.add bounds [start (+ start m)])
          (aset pos 0 (+ start m))))
      {:perm perm :bounds (vec bounds)})))

(def ^:private elementwise-op-kws
  "Ops that compute row-independently — safe to evaluate once over the whole
  reordered dataset. Excludes aggregators (mn/md/qnt/…), which in window-mode :set
  are GROUP reductions broadcast to the group's rows, so must run per group."
  #{:+ :- :* :div :div0 :sq :log :> :< :>= :<= := :and :or :not :in :between?
    :abs :exp :sqrt :pow :floor :ceil :round :signum
    :asinh :na2zero :neg2na :nonfin2na :when-finite
    :year :month :day :dow :quarter})

(defn- element-wise-ast?
  "True if `node` is purely element-wise (no aggregation / window / group reduction),
  so it can run once over the whole reordered base instead of per group. Conservative:
  unknown node types → false (run per group, always correct)."
  [node]
  (case (:node/type node)
    (:col :lit :binding-ref) true
    :op (and (contains? elementwise-op-kws (:op/name node))
             (every? element-wise-ast? (:op/args node)))
    :if (every? element-wise-ast? [(:if/pred node) (:if/then node) (:if/else node)])
    :coalesce (every? element-wise-ast? (:coalesce/args node))
    :let (and (every? (comp element-wise-ast? :binding/expr) (:let/bindings node))
              (element-wise-ast? (:let/body node)))
    false))

(defn- single-col-win-spec
  "If `dval` is a *pure single-column* window op — an AST `:win` node whose first arg
  is a column ref and whose remaining args are literals (e.g. `(win/mavg :x 4)` or
  `(win/mdev :x 12 {:ddof 0})`) — return {:win-fn :col :args} for the allocation-light
  reader-slicing path. Otherwise nil (composite/aggregator → the general path)."
  [dval]
  (let [node (deriv-ast dval)]
    (when (and node (= :win (:node/type node)))
      (let [args (:win/args node)
            c (first args)]
        (when (and c (= :col (:node/type c))
                   (every? #(= :lit (:node/type %)) (rest args)))
          (when-let [f (expr/win-op-fn (:win/op node))]
            {:win-fn f :col (:col/name c) :args (mapv :lit/value (rest args))}))))))

(defn- window-derive
  "Compute a per-group derivation (window op, or a group aggregator broadcast to the
  group's rows) over each contiguous group slice of the already-reordered `base`, and
  assemble a full-length column **in original input order** — each group's results are
  scattered back to their original row positions via `perm` (perm[i] = the input row at
  base position i). So `:within-order` governs only the per-group computation order, not
  the output order; the output keeps input row order.

  Pure single-column window ops take a primitive reader-slicing path: the source
  column is pulled once and each group is a zero-copy `dtype/sub-buffer` slice handed
  straight to the window fn — no per-group sub-datasets. Everything else (composite
  #dt/e, aggregator broadcast) falls back to a narrow per-group `ds/select-rows` view
  + `derive-column`; a scalar result (an aggregator) is broadcast across the group."
  [base perm bounds col-kw dval]
  (let [n (ds/row-count base)
        out (object-array n)
        ^ints p perm]
    (if-let [{:keys [win-fn col args]} (single-col-win-spec dval)]
      (let [src (dtype/->reader (ds/column base col))]
        (doseq [[s e] bounds]
          (let [len (- (long e) (long s))
                slice (dtype/sub-buffer src s len)
                res (dtype/->reader (apply win-fn slice args))]
            ;; scatter each result to its ORIGINAL row position (perm[s+j])
            (dotimes [j len] (aset out (aget p (+ (long s) j)) (nth res j))))))
      (let [refs (some-> (deriv-ast dval) expr/col-refs seq vec)
            ;; narrow only to columns that exist — an unknown ref stays absent from
            ;; the view so derive-column raises the structured :unknown-column error
            ;; (with suggestions) instead of a raw tech "column not found"
            present (when refs (filterv (clojure.core/set (ds/column-names base)) refs))
            ;; only narrow when every ref exists — otherwise keep the full view so
            ;; the structured error suggests against all available columns
            wbase (if (and refs (= (count present) (count refs)))
                    (ds/select-columns base present)
                    base)]
        (doseq [[s e] bounds]
          (let [len (- (long e) (long s))
                sub (ds/select-rows wbase (int-array (range s e)))
                res (derive-column sub col-kw dval)]
            (if (dtype/reader? res)
              (let [rdr (dtype/->reader res)]
                (dotimes [j len] (aset out (aget p (+ (long s) j)) (nth rdr j))))
              (dotimes [j len] (aset out (aget p (+ (long s) j)) res)))))))   ; scalar → broadcast
    out))

(def ^:private native-dtype-family
  "Native datatype family a packed column's dtype maps to for off-heap relocation."
  {:int8 :int64 :int16 :int64 :int32 :int64 :int64 :int64
   :uint8 :int64 :uint16 :int64 :uint32 :int64 :uint64 :int64
   :float32 :float64 :float64 :float64})

(defn- materialize-derived
  "Build the derived column. First lets tech.ml.dataset pack `raw` into a column —
  deciding dtype + missing-set exactly as the on-heap path does (NaN/nil → missing,
  integer-valued → :int64, etc.). Then, with `off-heap?` (§2.11(b)), it relocates a
  numeric column's data into an off-heap native buffer of the same dtype family,
  carrying the same missing-set (GC-tracked, so it frees when the dataset is GC'd) —
  taking a wide `:set` from ~6 GB on-heap to ~0 JVM heap. Because it relocates the
  already-packed column, off-heap output is identical to on-heap by construction.
  Non-numeric columns and the on-heap default are returned as built."
  [col-kw raw off-heap?]
  (let [col (ds/new-column col-kw raw)]
    (if-let [ndt (and off-heap? (native-dtype-family (dtype/elemwise-datatype col)))]
      (let [n (ds/row-count col)
            buf (dtype/make-container :native-heap ndt {:resource-type :gc} n)]
        (dtype/copy! (dtype/->reader col ndt) buf)
        (ds/new-column col-kw buf {} (ds/missing col)))
      col)))

(defn- fast-group-set
  "Fast path for keyword-only :by window-mode :set (map or independent
  seq-of-pairs derivations). Computes the
  grouped + `:within-order`-sorted permutation once and a `base` view of the source
  columns in that order (for contiguous per-group window computation, no per-group
  sub-datasets of every column, no `ds/concat`). Output is in **original input row
  order**: passthrough columns are the original dataset untouched; window/aggregator
  derivations scatter their per-group results back to original positions via the
  permutation; element-wise derivations are computed directly on the original dataset.
  `:within-order` thus orders only the per-group computation, not the output. With
  `off-heap?`, numeric derived columns are materialised off-heap (§2.11(b)). A
  precomputed `grouping` ({:perm :bounds} from `prepare-grouping`) is reused as-is,
  skipping the per-call grouping + sort (the amortised multi-pass path)."
  [dataset by derivations within-order off-heap? grouping]
  (when (and within-order (not grouping))
    (let [available (set (ds/column-names dataset))
          unknown (vec (remove available (map order-spec-col within-order)))]
      (when (seq unknown)
        (throw (ex-info (str "Unknown column(s) " unknown " in :within-order expression")
                        {:dt/error :unknown-column :dt/columns unknown
                         :dt/context :within-order :dt/available (vec (sort available))})))))
  (let [{:keys [perm bounds]} (or grouping (group-perm dataset by within-order))
        base (ds/select-rows dataset perm)]
    (reduce (fn [d [col-kw dval]]
              (let [ast (deriv-ast dval)
                    raw (if (element-wise-ast? ast)
                          (derive-column dataset col-kw dval)            ; original order, direct
                          (window-derive base perm bounds col-kw dval))] ; scattered to original
                (ds/add-column d (materialize-derived col-kw raw off-heap?))))
            dataset
            (seq derivations))))

(defn- independent-derivations?
  "True when the fast :by path computes the same result as sequential/simultaneous
  application: every derivation is an introspectable expression (no plain fns) and
  no derivation references a column derived by an EARLIER pair (map entries never
  see siblings — validate-map-set-cross-refs rejects cross-refs — so a map only
  needs the AST check). Independence makes sequential ≡ simultaneous, so a
  seq-of-pairs :set qualifies for the fast path; pair order still fixes column order."
  [derivations]
  (if (map? derivations)
    (every? #(deriv-ast (val %)) derivations)
    (loop [ps (seq derivations) derived #{}]
      (if-let [[col-kw dval] (first ps)]
        (let [ast (deriv-ast dval)]
          (and (some? ast)
               (empty? (set/intersection (expr/col-refs ast) derived))
               (recur (next ps) (conj derived col-kw))))
        true))))

(defn- apply-group-set [dataset by derivations within-order off-heap? grouping]
  (if (zero? (ds/row-count dataset))
    dataset
    (if (and (keyword-only-by? by)
             (independent-derivations? derivations))
      ;; Fast path: keyword-only :by + independent derivations — reorder once, no concat.
      (fast-group-set dataset by derivations within-order off-heap? grouping)
      ;; General path. Output stays in original input order: tag each row with an
      ;; index, group/sort/compute/concat (grouped order), then sort back by the index
      ;; and drop it. Compound case (cut marker + exact keys): partition by exact
      ;; keys first so cut breakpoints are computed per sub-dataset.
      (do
        (when (keyword-only-by? by)
          (info-note :group-set-general-path
                     (str ":set with :by took the general per-group path (cross-referencing"
                          " pairs or plain-fn derivations) — it builds per-group sub-datasets"
                          " over ALL columns, which is slow and memory-hungry on wide data."
                          " Independent expression derivations ride the fast one-pass path.")))
        (let [n (ds/row-count dataset)
              indexed (ds/add-column dataset (ds/new-column ::orig-idx (int-array (range n))))
              partitions (if (needs-per-partition-resolution? by)
                           (let [exact-keys (filterv keyword? by)]
                             (vals (ds/group-by indexed (fn [row] (select-keys row exact-keys)))))
                           [indexed])]
          (-> (->> partitions
                   (mapcat (fn [partition-ds]
                             (let [group-fn (by->group-fn partition-ds by)
                                   groups (ds/group-by partition-ds group-fn)]
                               (map (fn [[_group-key sub-ds]]
                                      (let [sorted (if within-order (apply-order-by sub-ds within-order :within-order) sub-ds)]
                                        (apply-set sorted derivations)))
                                    groups))))
                   (apply ds/concat))
              (ds/sort-by-column ::orig-idx)
              (dissoc ::orig-idx)))))))

(defn- apply-window-set
  "Window mode without :by — the entire dataset is one partition. Same output
  contract as :by window mode (2.7.0): `:within-order` governs only the order
  the window computation walks the rows; results are scattered back to their
  original positions, so the output keeps **input row order** (use :order-by to
  sort). Element-wise derivations are computed directly in input order;
  window/aggregator derivations run over the (optionally sorted) single
  partition; sequential (seq-of-pairs) derivations see earlier-derived columns.
  Numeric derived columns are materialised off-heap by default, like the :by
  fast path."
  [dataset derivations within-order off-heap?]
  (let [n (ds/row-count dataset)]
    (if (zero? n)
      dataset
      (let [perm (or (when within-order (order-perm dataset within-order :within-order))
                     (int-array (range n)))
            bounds [[0 n]]]
        (reduce (fn [d [col-kw dval]]
                  (let [ast (deriv-ast dval)
                        raw (if (element-wise-ast? ast)
                              (derive-column d col-kw dval)
                              (window-derive (ds/select-rows d perm) perm bounds col-kw dval))]
                    (ds/add-column d (materialize-derived col-kw raw off-heap?))))
                dataset
                (if (map? derivations) (seq derivations) derivations))))))

(defn prepare-grouping
  "Precompute the grouping + `:within-order` permutation for `dataset` so it can be
  reused across many `:set` + `:by` passes (e.g. a multi-pass per-entity ETL),
  amortising the grouping + sort that each pass would otherwise repeat. `by` is a
  non-empty vector of keyword column names; `within-order` is an optional sort spec
  (same form as `:within-order`). Pass the result directly as `:by` to `dt`, in
  place of the column vector + `:within-order`:

      (let [g (prepare-grouping ds [:gvkey] [(asc :datadate)])]
        (-> ds (dt :set {…} :by g) (dt :set {…} :by g) …))

  The grouping stays valid for any dataset with the SAME rows in the SAME order —
  adding columns between passes is fine, since `:set :by` preserves input row order.
  `dt` checks the row count matches; reusing it on reordered rows is undefined."
  ([dataset by] (prepare-grouping dataset by nil))
  ([dataset by within-order]
   (when-not (and (sequential? by) (seq by) (every? keyword? by))
     (throw (ex-info "prepare-grouping :by must be a non-empty vector of column keywords."
                     {:dt/error :invalid-grouping-by :dt/by by})))
   (let [available (set (ds/column-names dataset))
         unknown (vec (remove available
                              (concat by (when within-order
                                           (map order-spec-col within-order)))))]
     (when (seq unknown)
       (throw (ex-info (str "Unknown column(s) " unknown " in prepare-grouping")
                       {:dt/error :unknown-column :dt/columns unknown
                        :dt/available (vec (sort available))})))
     (let [{:keys [perm bounds]} (group-perm dataset (vec by) within-order)]
       {:dt/selector :grouping
        :by (vec by) :within-order within-order :perm perm :bounds bounds
        :row-count (ds/row-count dataset)}))))

(defn- grouping?
  "True for a prepared grouping produced by `prepare-grouping` (accepted as :by)."
  [x]
  (and (map? x) (= :grouping (:dt/selector x))))

(def ^:private shown-notes (atom #{}))

(defn- info-note
  "Print a one-time informational note identified by key.
  Subsequent calls with the same key are silent."
  [key msg]
  (when-not (contains? @shown-notes key)
    (swap! shown-notes conj key)
    (println (str "[datajure] NOTE: " msg))))

(defn reset-notes!
  "Reset shown info notes. Useful for testing."
  []
  (reset! shown-notes #{}))

(def ^:dynamic *dt*
  "Holds the last dataset result in an interactive REPL session.
  Automatically bound by datajure.nrepl/wrap-dt middleware.
  Like Clojure's *1, but only for tech.v3.dataset results."
  nil)

(def N
  "Row count aggregation helper. Use as a value in :agg maps.
  Terse alias matching data.table/q convention. See also `nrow` for
  a more discoverable full name."
  ds/row-count)

(def nrow
  "Row count aggregation helper. Use as a value in :agg maps.
  Full-name alias for users who prefer readability over terseness.
  Equivalent to `N`."
  ds/row-count)

(def mean
  "Column mean. Full-name alias for `dfn/mean`."
  dfn/mean)

(def sum
  "Column sum. Full-name alias for `dfn/sum`."
  dfn/sum)

(defn median
  "Column median (R type-7, matching R's `median`). Drops nil and non-finite
  values. Equivalent to `(qnt col 0.5)`."
  [col]
  (math/quantile-type7 col 0.5))

(defn qnt
  "Column R type-7 p-quantile (p a fraction in [0,1]); matches R's
  `quantile(x, p, type = 7, na.rm = TRUE)`. Drops nil and non-finite values.
  With `min-n`, returns nil when fewer than `min-n` finite values remain
  (floor-free by default). `p` may be a vector of probabilities, in which case
  the column is sorted once and a vector of quantiles is returned (the efficient
  q20/median/q80 band form). Also available as the `qnt` op in #dt/e and as an
  `:agg`/`:set` data-form `[:qnt :col p]`."
  ([col p] (if (sequential? p) (math/quantiles-type7 col p) (math/quantile-type7 col p)))
  ([col p min-n] (if (sequential? p)
                   (math/quantiles-type7 col p min-n)
                   (math/quantile-type7 col p min-n))))

(def stddev
  "Column standard deviation. Full-name alias for `dfn/standard-deviation`."
  dfn/standard-deviation)

(def variance
  "Column variance. Full-name alias for `dfn/variance`."
  dfn/variance)

(def max*
  "Column maximum, skipping nil/missing; nil for an all-missing column.
  Asterisk-suffixed to avoid shadowing `clojure.core/max`.
  Delegates to `expr/col-max` — `dfn/reduce-max` returns a wrong value when
  the column has missing entries (a missing slot corrupts the reduction)."
  expr/col-max)

(def min*
  "Column minimum, skipping nil/missing; nil for an all-missing column.
  Asterisk-suffixed to avoid shadowing `clojure.core/min`.
  Delegates to `expr/col-min` (see `max*` for the `dfn/reduce-min` caveat)."
  expr/col-min)

(def prod
  "Product of the non-nil values in a column; nil for an all-missing column
  (an empty product of 1 would be misleading when every observation is missing).
  Delegates to `expr/col-prod` (shared with the #dt/e `prod` op)."
  expr/col-prod)

(def count*
  "Count of non-nil values in a column.
  Asterisk-suffixed to avoid shadowing `clojure.core/count`.
  Distinct from N (total rows) and count-distinct (unique non-nil values).
  Delegates to `expr/count-non-nil` (shared with the #dt/e `:ct` op)."
  expr/count-non-nil)

(def div0
  "Nil-safe division of two scalars: nil when either is nil or the denominator
  is zero, else `num`/`den` as a double. Use in plain-fn contexts (`:set`/`:agg`
  with `#(...)`, computed `:by`) where the #dt/e `div0` op isn't available; the
  op delegates to this same fn. Non-numeric inputs throw normally.
  Examples: (div0 1 2) => 0.5; (div0 1 0) => nil; (div0 1 nil) => nil."
  expr/div0)

(defn pass-nil
  "Wraps a row-level fn to return nil if any of the specified guard columns
  are nil/missing in the row. Prevents crashes when plain fns encounter
  missing values in :set or :where.

  Usage: (pass-nil #(Integer/parseInt (:x-str %)) :x-str)"
  [f & guard-cols]
  (fn [row]
    (if (some nil? (map #(get row %) guard-cols))
      nil
      (f row))))

(defn rename
  "Rename columns in a dataset without dropping any.
  col-map is {old-kw new-kw}."
  [dataset col-map]
  (ds/rename-columns dataset col-map))

(defn xbar
  "Floor-division bucketing — floors a column value to the nearest multiple of width.
  Inspired by q's xbar operator.

  For numeric columns: (xbar :price 10) → floor(:price / 10) * 10
  For temporal columns: (xbar :time 5 :minutes) → floor to nearest 5-minute boundary

  Supported temporal units: :seconds, :minutes, :hours, :days, :weeks
  (singular spellings — :second, :minute, … — are accepted everywhere)

  Primary use case: computed :by grouping for time-series bar generation.

  Usage:
    ;; Numeric bucketing in :by
    (dt ds :by [(xbar :price 10)] :agg {:n N :avg #dt/e (mn :volume)})

    ;; 5-minute OHLCV bars
    (-> trades
        (dt :order-by [(asc :time)])
        (dt :by [(xbar :time 5 :minutes) :sym]
            :agg {:open  #dt/e (first-val :price)
                  :close #dt/e (last-val :price)
                  :vol   #dt/e (sm :size)
                  :n     N}))

    ;; Also usable inside #dt/e as a column derivation:
    (dt ds :set {:bucket #dt/e (xbar :price 5)})"
  ([col-kw width]
   (with-meta
     (fn [row]
       (let [v (get row col-kw)]
         (when (some? v)
           (* width (Math/floorDiv (long v) (long width))))))
     {:xbar/col col-kw}))
  ([col-kw width unit]
   (let [ms-per-unit (or (some->> unit (math/canonical-unit math/ms-per-unit) math/ms-per-unit)
                         (throw (ex-info (str "Unknown xbar temporal unit: " unit
                                              ". Supported: :seconds :minutes :hours :days :weeks"
                                              " (singular spellings accepted).")
                                         {:dt/error :xbar-unknown-unit :unit unit})))]
     (with-meta
       (fn [row]
         (let [v (get row col-kw)]
           (when (some? v)
             (let [epoch-ms (tech.v3.datatype.datetime/datetime->epoch :epoch-milliseconds v)
                   epoch-units (Math/floorDiv ^long epoch-ms ^long ms-per-unit)]
               (* width (Math/floorDiv ^long epoch-units ^long width))))))
       {:xbar/col col-kw}))))

(defn cut
  "Equal-count (quantile) binning — one name, two contexts, exactly like `xbar`:

    * Inside #dt/e (`:set`/`:where`/`:agg`): a column of bin integers —
      `(dt ds :set {:quintile #dt/e (cut :mktcap 5)})`.
    * Standalone in :by: a grouping marker that bins each row —
      `(dt ds :by [(cut :mktcap 5)] :agg {...})`.

  Both contexts share the same breakpoints (R type-7 quantiles at 1/n .. (n-1)/n,
  values equal to a breakpoint fall in the lower bin) so they always bin
  identically for the same population. Inspired by R's `cut` and Stata's `xtile`.
  nil input values produce nil (their own group in :by).

  Breakpoint population in :by — depends on what else is in :by:
    * cut alone in :by               → breakpoints from the WHOLE dataset
    * cut + other exact keys in :by  → breakpoints are computed PER
                                       exact-key partition

  So `:by [:date (cut :mktcap 5)]` does what you would expect in data.table
  or dplyr: each date's rows are binned against that date's own quintiles.
  This is the canonical CRSP / Fama-French pattern — per-date cross-sectional
  size quintiles.

  The optional :from keyword accepts a #dt/e boolean expression or a boolean
  column keyword selecting a reference subpopulation for breakpoint
  computation (same as :from inside #dt/e). When combined with other exact
  keys in :by, the mask is applied within each partition. Classic NYSE use case:

      (dt stocks :by [:date (cut :mktcap 5 :from #dt/e (= :exchcd 1))]
          :agg {:mean-ret #dt/e (mn :ret)})

  per-date NYSE quintile breakpoints applied to all stocks (NYSE + AMEX +
  NASDAQ) — Fama-French size sort exactly.

  Companion to `xbar` (equal-width bins).

  In :by, the result column name defaults to `<col>-q<n>` (e.g. :mktcap-q5).
  Override via :datajure/col metadata on the marker. Inside #dt/e you name
  the column yourself in :set.

  Note on small partitions: if a partition has fewer than n non-nil values,
  breakpoints cannot be computed and all non-nil rows in that partition
  land in bin 1. Consider filtering out thin partitions upstream or using
  fewer bins.

  Usage:
    ;; Column of quintile bins
    (dt ds :set {:size-q #dt/e (cut :mktcap 5)})

    ;; Global quintiles as a grouping
    (dt stocks :by [(cut :mktcap 5)]
        :agg {:n N :mean-ret #dt/e (mn :ret)})

    ;; Per-date size quintiles — the canonical CRSP / Fama-French pattern
    (dt stocks :by [:date (cut :mktcap 5)]
        :agg {:mean-ret #dt/e (mn :ret)})

    ;; Per-date NYSE quintile breakpoints applied to all stocks
    (dt stocks :by [:date (cut :mktcap 5 :from #dt/e (= :exchcd 1))]
        :agg {:mean-ret #dt/e (mn :ret)})"
  [col-kw n & {:keys [from]}]
  (when-not (and (integer? n) (pos? n))
    (throw (ex-info (str "cut requires a positive integer n, got: " n)
                    {:dt/error :cut-invalid-n :n n})))
  (when-not (keyword? col-kw)
    (throw (ex-info (str "cut requires a column keyword, got: " col-kw)
                    {:dt/error :cut-invalid-col :col col-kw})))
  (cond-> {:dt/selector :cut
           :dt/col col-kw
           :dt/n n
           :datajure/col (keyword (str (name col-kw) "-q" n))}
    (some? from) (assoc :dt/from from)))

(defn col-range
  "Returns a column selector that selects all columns positionally between
  start-col and end-col (inclusive). Both endpoints must exist in the dataset.
  Intended for use with :select in dt. (Named col-range — a POSITIONAL range
  over column names — to keep it visually distinct from the #dt/e value
  predicate `between?`.)

  Example:
    (dt ds :select (col-range :month-01 :month-12))"
  [start-col end-col]
  {:dt/selector :col-range
   :dt/start start-col
   :dt/end end-col})

(defn asc
  "Sort-spec helper: ascending order on col. Use in :order-by."
  [col]
  {:order :asc :col col})

(defn desc
  "Sort-spec helper: descending order on col. Use in :order-by."
  [col]
  {:order :desc :col col})

(defn- validate-order-specs
  "Validates :order-by / :within-order specs and returns them normalised. Each
  spec must be a bare column keyword or a map {:order :asc|:desc :col <col>}, and
  every referenced column must exist (structured :unknown-column error with
  suggestions, same as :select). Throws :invalid-order-spec on a malformed spec."
  [dataset specs context]
  (let [normalised (mapv normalise-order-spec specs)]
    (doseq [s normalised]
      (when-not (and (map? s) (contains? s :col))
        (throw (ex-info (str "Invalid " context " spec: " (pr-str s)
                             " — expected a column keyword or (asc col)/(desc col).")
                        {:dt/error :invalid-order-spec :dt/context context :dt/spec s})))
      (when-not (#{:asc :desc} (:order s))
        (throw (ex-info (str "Invalid " context " order: " (pr-str (:order s))
                             " in " (pr-str s) " — expected :asc or :desc.")
                        {:dt/error :invalid-order-spec :dt/context context :dt/spec s}))))
    (validate-select-cols dataset (map :col normalised) context)
    normalised))

(defn- order-perm
  "Stable sort permutation of `dataset`'s rows by `specs` (per-key :asc/:desc),
  as an int-array, or nil when sorting is a no-op (fewer than 2 rows or no
  specs). Reads only the sort-key columns and compares with
  `clojure.core/compare` (nils first, mixed asc/desc). `context` labels
  validation errors (:order-by or :within-order)."
  [dataset specs context]
  (let [normalised (validate-order-specs dataset specs context)
        n          (ds/row-count dataset)]
    (when-not (or (< n 2) (empty? normalised))
      (let [key-vals (mapv (fn [{:keys [col]}] (vec (ds/column dataset col))) normalised)
            descs    (mapv #(= :desc (:order %)) normalised)
            nk       (count normalised)
            cmp      (fn [ia ib]
                       (let [i (int ia) j (int ib)]
                         (loop [k 0]
                           (if (< k nk)
                             (let [raw (clojure.core/compare (nth (key-vals k) i)
                                                             (nth (key-vals k) j))
                                   c   (if (nth descs k) (- raw) raw)]
                               (if (zero? c) (recur (inc k)) c))
                             0))))]
        (int-array (sort cmp (range n)))))))

(defn- apply-order-by
  "Sort `dataset` by `specs` via `order-perm` + a single `ds/select-rows` gather.
  Avoids tech's row-map `sort-by` path, which materialises a full row object per
  row even though only the key columns are compared — catastrophic for wide
  datasets."
  [dataset specs context]
  (if-let [p (order-perm dataset specs context)]
    (ds/select-rows dataset p)
    dataset))

(defn- apply-take
  "Row limit. Positive `n` keeps the first `n` rows (head); negative keeps the
  last `|n|` (tail); `0` yields an empty dataset. `|n|` larger than the row count
  returns all rows. Runs last, after :order-by."
  [dataset n]
  (if (neg? n)
    (ds/tail dataset (- n))
    (ds/head dataset n)))

(defn- validate-select-cols
  "Checks that all requested columns exist in the dataset. Throws ex-info with
  Levenshtein suggestions on unknown columns. `context` (default :select) labels
  the error so the same check serves :select, :order-by, and :within-order."
  ([dataset requested] (validate-select-cols dataset requested :select))
  ([dataset requested context]
   (let [available (set (ds/column-names dataset))
         unknown (set/difference (set requested) available)]
     (when (seq unknown)
       (let [avail-names (vec (sort available))
             suggestions (into {}
                               (map (fn [col]
                                      (let [col-str (name col)
                                            closest (->> avail-names
                                                         (map (fn [a] [a (expr/damerau-levenshtein col-str (name a))]))
                                                         (sort-by second)
                                                         first)]
                                        [col (when (and closest (<= (second closest) 3)) [(first closest)])])))
                               unknown)]
         (throw (ex-info (str "Unknown column(s) " unknown " in " context)
                         {:dt/error :unknown-column
                          :dt/columns unknown
                          :dt/context context
                          :dt/available avail-names
                          :dt/closest suggestions})))))))

(defn- apply-select [dataset selector]
  (let [all-cols (ds/column-names dataset)
        col-dtype (fn [col-kw]
                    (-> (ds/column dataset col-kw) meta :datatype))]
    (cond
      (and (map? selector) (= :col-range (:dt/selector selector)))
      (let [{:dt/keys [start end]} selector
            _ (validate-select-cols dataset [start end])
            all-names (vec all-cols)
            si (.indexOf all-names start)
            ei (.indexOf all-names end)]
        (when (neg? si)
          (throw (ex-info (str "col-range: start column " start " not found")
                          {:dt/error :unknown-column :dt/columns #{start}})))
        (when (neg? ei)
          (throw (ex-info (str "col-range: end column " end " not found")
                          {:dt/error :unknown-column :dt/columns #{end}})))
        (let [[lo hi] (if (<= si ei) [si ei] [ei si])]
          (ds/select-columns dataset (subvec all-names lo (inc hi)))))

      (map? selector)
      (do
        (validate-select-cols dataset (keys selector))
        (-> dataset
            (ds/select-columns (keys selector))
            (ds/rename-columns selector)))

      (and (vector? selector) (= :not (first selector)))
      (let [excluded (set (rest selector))]
        (validate-select-cols dataset excluded)
        (ds/select-columns dataset (remove excluded all-cols)))

      (vector? selector)
      (do
        (validate-select-cols dataset selector)
        (ds/select-columns dataset selector))

      (= :type/numerical selector)
      (ds/select-columns dataset (filter #(casting/numeric-type? (col-dtype %)) all-cols))

      (= :!type/numerical selector)
      (ds/select-columns dataset (remove #(casting/numeric-type? (col-dtype %)) all-cols))

      (keyword? selector)
      (do
        (validate-select-cols dataset [selector])
        (ds/select-columns dataset [selector]))

      (instance? java.util.regex.Pattern selector)
      (ds/select-columns dataset (filter #(re-find selector (name %)) all-cols))

      (fn? selector)
      (ds/select-columns dataset (filter selector all-cols))

      :else
      (throw (ex-info "Invalid :select argument" {:selector selector})))))

(def ^:private dt-query-keys
  #{:where :set :agg :by :select :order-by :within-order :take :off-heap})

(defn- dt-query-map
  "Normalise dt's arguments to a single query map. Accepts kwargs
  (:where p :by [...]), a single query map ({:where p :by [...]}), or kwargs
  with a trailing map — and rejects unknown query keys with a structured
  error + suggestion, instead of silently ignoring a typo like :wehre."
  [args]
  (let [query (cond
                (and (= 1 (count args)) (map? (first args)))
                (first args)

                (even? (count args))
                (apply array-map args)

                (map? (last args))
                (merge (apply array-map (butlast args)) (last args))

                :else
                (throw (ex-info "dt takes keyword/value pairs and/or a query map after the dataset."
                                {:dt/error :invalid-dt-args :dt/args (vec args)})))
        unknown (remove dt-query-keys (keys query))]
    (when (seq unknown)
      (let [suggestions (into {}
                              (keep (fn [k]
                                      (when (keyword? k)
                                        (let [[best d] (->> dt-query-keys
                                                            (map (fn [q] [q (expr/damerau-levenshtein (name k) (name q))]))
                                                            (sort-by second)
                                                            first)]
                                          (when (<= d 2) [k best])))))
                              unknown)]
        (throw (ex-info (str "Unknown dt query key(s) " (vec unknown) "."
                             (when (seq suggestions)
                               (str " Did you mean: "
                                    (clojure.string/join ", " (map (fn [[k v]] (str k " -> " v)) suggestions))
                                    "?"))
                             " Supported: " (vec (sort dt-query-keys)) ".")
                        {:dt/error :unknown-query-key
                         :dt/keys (vec unknown)
                         :dt/suggestions suggestions
                         :dt/supported (vec (sort dt-query-keys))}))))
    query))

(defn dt
  "Query a dataset. Supported keywords: :where, :set, :agg, :by, :select,
  :order-by, :within-order, :take, :off-heap.

  Arguments may be given as keyword/value pairs or as a single query map —
  (dt ds :where p :by [:g] :agg {...}) and (dt ds {:where p :by [:g] :agg {...}})
  are equivalent. With data-form expressions, a whole query is plain EDN data
  that can be stored, merged, and built programmatically. Unknown query keys
  throw a structured :unknown-query-key error (with a typo suggestion) instead
  of being silently ignored.

  :where         - filter rows. Accepts a #dt/e expression, a runtime data-form
                   vector (e.g. [:= :tic ticker] — keywords are columns, anything
                   else is a literal value, so runtime values flow in without a
                   row-map), or a plain fn of the row map.
  :set           - derive/update columns. Accepts a map (simultaneous) or any
                   seq of pairs (sequential — later pairs see earlier-derived columns).
                   When :set contains win/* functions, window mode is activated —
                   with :by, computes within groups; without :by, whole dataset is one partition.
                   With :by, a map — or a seq of pairs whose expressions never reference
                   a column derived by an earlier pair — runs on the fast one-pass path;
                   genuinely cross-referencing or plain-fn derivations fall back to the
                   per-group path (slow on wide data; a one-time NOTE says so).
  :agg           - collapse to summary. Accepts map or seq-of-pairs. Use (nrow)/[:nrow] for row count.
  :by            - grouping for :agg or :set (partitioned window mode). A vector of
                   keywords, a fn of the row, or (for :set) a prepared grouping from
                   `prepare-grouping` — which bundles the group keys and the
                   :within-order sort, amortising them across multi-pass transforms.
                   #dt/e expressions / data-form vectors are NOT accepted as entries
                   (structured :expr-in-by error) — derive the key with :set first.
  :within-order  - the order rows are WALKED within each partition (or across the
                   whole dataset when :by is absent) while :set or :agg computes —
                   for window functions (win/lag, win/cumsum, ...) and
                   order-sensitive aggregations (first-val, last-val, OHLC).
                   It never affects output row order: a :set query returns rows in
                   input order regardless (use :order-by to sort output); an :agg
                   query returns one row per group as usual.
  :select        - keep columns. Accepts: vector of kws, single kw, [:not kw ...],
                   regex, predicate fn, or map {old-kw new-kw} for rename-on-select.
  :order-by      - sort rows. Accepts a vector of (asc :col)/(desc :col) specs,
                   or bare keywords (default asc). Evaluated before :take.
  :take          - row limit (integer). Positive n keeps the first n rows (head),
                   negative keeps the last |n| (tail), 0 yields no rows. |n| beyond
                   the row count returns all rows. Evaluated last, after :order-by —
                   e.g. :order-by [(asc :date)] :take -20 is \"the last 20 by date\".
  :off-heap      - boolean, default true. For window-mode :set (keyword-only :by
                   fast path, prepared-grouping :by, or whole-dataset windows),
                   materialise numeric derived columns in off-heap native buffers
                   (freed on GC, type-preserving int/float) instead of on the JVM
                   heap — for wide per-group transforms this takes the result from
                   gigabytes of heap to ~0. Pass :off-heap false for on-heap output.
                   No effect on other query shapes or non-numeric derived columns."
  [dataset & args]
  (let [{:keys [where set agg by select order-by within-order take off-heap]
         :or {off-heap true}} (dt-query-map args)
        grouping (when (grouping? by) by)]
    (when (and set agg)
      (throw (ex-info "Cannot combine :set and :agg in the same dt call. Use -> threading for multi-step queries."
                      {:dt/error :set-agg-conflict})))
    (when (and (some? take) (not (integer? take)))
      (throw (ex-info (str ":take requires an integer (got " (pr-str take)
                           "). Positive = first n rows, negative = last n.")
                      {:dt/error :invalid-take :dt/value take})))
    (when (and within-order (not set) (not agg))
      (throw (ex-info ":within-order requires :set or :agg."
                      {:dt/error :within-order-invalid})))
    (when grouping
      (when (or agg (not set))
        (throw (ex-info "a prepared grouping as :by requires :set (it is not for :agg)."
                        {:dt/error :grouping-requires-set})))
      (when within-order
        (throw (ex-info "a prepared grouping already encodes :within-order — don't pass it alongside."
                        {:dt/error :grouping-conflict})))
      (when (not= (:row-count grouping) (ds/row-count dataset))
        (throw (ex-info (str "the prepared grouping was built for " (:row-count grouping) " rows but the dataset has "
                             (ds/row-count dataset) " — it must be reused on the same rows in the same order.")
                        {:dt/error :grouping-row-mismatch
                         :dt/expected (:row-count grouping) :dt/actual (ds/row-count dataset)}))))
    (let [where (if (vector? where) (expr/data->ast where) where)
          set (normalise-derivations set)
          agg (normalise-derivations agg)
          eff-by (if grouping (:by grouping) by)
          _ (validate-by-entries eff-by)
          eff-wo (if grouping (:within-order grouping) within-order)
          set-has-win? (and set (derivations-have-win? set))
          window-mode? (and eff-by set (not agg))]
      (when (and where (expr-node? where))
        (validate-no-win where :where))
      (when (and set (not window-mode?) (not set-has-win?))
        (validate-win-in-derivations set :set)
        (validate-map-set-cross-refs set))
      (when (and set window-mode?)
        (validate-map-set-cross-refs set))
      (when (and set set-has-win? (not by))
        (validate-map-set-cross-refs set))
      (when agg
        (validate-win-in-derivations agg :agg))
      (when (and agg (not by))
        (info-note :agg-no-by "Aggregating over entire dataset. Use :by for group aggregation."))
      (when window-mode?
        (info-note :window-mode "Window mode: computing within groups, keeping all rows in input order.")
        (when (not eff-wo)
          (info-note :window-no-order "Window mode computes in current row order. Use :within-order to set the per-group computation order (output stays in input order).")))
      (when (and set-has-win? (not eff-by))
        (info-note :window-mode-no-by "Window mode (whole dataset): computing over entire dataset, keeping all rows in input order.")
        (when (not within-order)
          (info-note :window-no-order "Window mode computes in current row order. Use :within-order to set the per-group computation order (output stays in input order).")))
      (cond-> dataset
        where (apply-where where)
        (and set eff-by) (apply-group-set eff-by set eff-wo off-heap grouping)
        (and set (not eff-by) (or within-order set-has-win?)) (apply-window-set set within-order off-heap)
        (and set (not eff-by) (not within-order) (not set-has-win?)) (apply-set set)
        (and agg by) (apply-group-agg by agg within-order)
        (and agg (not by)) (apply-agg agg within-order)
        select (apply-select select)
        order-by (apply-order-by order-by :order-by)
        (some? take) (apply-take take)))))
