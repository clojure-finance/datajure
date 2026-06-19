(ns datajure.core
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [clojure.set :as set]
            [datajure.expr :as expr]
            [datajure.math :as math]))

(declare apply-order-by validate-select-cols)

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
  lower bin), so qtile and cut produce identical bins for values equal to a
  breakpoint. Returns nil for nil input."
  [v breakpoints]
  (when (some? v)
    (loop [i 0]
      (cond
        (>= i (count breakpoints)) (inc (count breakpoints))
        (<= v (nth breakpoints i)) (inc i)
        :else (recur (inc i))))))

(defn- resolve-qtile-marker
  "Given a {:dt/selector :qtile ...} marker and the dataset, compute
  breakpoints once and return a metadata-tagged row-fn that bins each row's
  value. The returned fn carries :datajure/col metadata so the resulting
  group-key column has a friendly name.

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
      (throw (ex-info (str "qtile: column " col-kw " not found in dataset")
                      {:dt/error :unknown-column
                       :dt/columns #{col-kw}
                       :dt/available (vec (sort (ds/column-names dataset)))})))
    (let [col (ds/column dataset col-kw)
          from-mask (when (some? from)
                      (dtype/->reader
                       (cond
                         (expr/expr-node? from) ((expr/compile-expr from) dataset)
                         (keyword? from) (ds/column dataset from)
                         :else (throw (ex-info "qtile :from must be a #dt/e expression or column keyword"
                                               {:dt/error :qtile-invalid-from :from from})))))
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

(defn- qtile-marker? [x]
  (and (map? x) (= :qtile (:dt/selector x))))

(defn- by->group-fn
  "Produce a row-to-group-key function from a :by spec. The dataset is required
  so that markers like qtile (which need population-level statistics) can
  precompute their breakpoints once before grouping."
  [dataset by]
  (cond
    (fn? by)
    by
    (every? keyword? by)
    (fn [row] (select-keys row by))
    :else
    (let [resolved (mapv (fn [item]
                           (if (qtile-marker? item)
                             (resolve-qtile-marker dataset item)
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

(defn- needs-per-partition-resolution?
  "True if :by mixes tagged markers (currently qtile) with exact keys — in
  which case markers must be resolved against each exact-key partition
  separately so breakpoints are per-group. Pure-marker :by (no exact keys)
  stays global because there is nothing to partition by. Pure-exact-key
  :by has no markers to resolve. Pure-fn :by is a user-controlled grouping
  and opts out of the marker machinery entirely."
  [by]
  (and (sequential? by)
       (some qtile-marker? by)
       (some keyword? by)))

(defn- keyword-only-by?
  "True for a :by that is a non-empty sequence of plain column keywords — no
  qtile/xbar markers and no fn. The fast group-agg path handles exactly this
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
                         (map #(if (keyword? %) % (:col %)) within-order))
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
         ;; General path. Compound case (qtile + exact keys): first partition by
         ;; exact keys so qtile breakpoints are computed per sub-dataset.
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
         (or (.get groups k) (let [a (java.util.ArrayList.)] (.put groups k a) a))
              (int i))))
    (let [cmp (when within-order
                (let [specs (mapv #(if (keyword? %) {:order :asc :col %} %) within-order)
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
    :asinh :na2zero :neg2na :nonfin2na})

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

(defn- window-derive
  "Compute a per-group derivation (window op, or a group aggregator broadcast to the
  group's rows) over each contiguous group slice of the already-reordered `base`, and
  assemble a full-length column. Narrows to the derivation's referenced columns so
  each group's `ds/select-rows` view covers a handful of columns, not the whole
  (passthrough-laden) dataset. A scalar result (an aggregator) is broadcast across
  the group; a per-row result (a window op) is placed elementwise."
  [base bounds col-kw dval]
  (let [refs (some-> (deriv-ast dval) expr/col-refs seq vec)
        wbase (if refs (ds/select-columns base refs) base)
        out (object-array (ds/row-count base))]
    (doseq [[s e] bounds]
      (let [len (- (long e) (long s))
            sub (ds/select-rows wbase (int-array (range s e)))
            res (derive-column sub col-kw dval)]
        (if (dtype/reader? res)
          (let [rdr (dtype/->reader res)]
            (dotimes [j len] (aset out (+ (long s) j) (nth rdr j))))
          (dotimes [j len] (aset out (+ (long s) j) res)))))   ; scalar → broadcast
    out))

(defn- fast-group-set
  "Fast path for keyword-only :by window-mode :set (map derivations). Computes the
  grouped+sorted permutation once, reorders ALL columns in a single `ds/select-rows`
  (no per-group sub-datasets of every column, no `ds/concat` — the §2.10 fix adapted
  to window mode), then computes each derivation against the reordered base: window
  derivations per contiguous group slice, element-wise derivations once over the
  whole base. Same result (grouped+within-order order) as the general path."
  [dataset by derivations within-order]
  (when within-order
    (let [available (set (ds/column-names dataset))
          unknown (vec (remove available (map #(if (keyword? %) % (:col %)) within-order)))]
      (when (seq unknown)
        (throw (ex-info (str "Unknown column(s) " unknown " in :within-order expression")
                        {:dt/error :unknown-column :dt/columns unknown
                         :dt/context :within-order :dt/available (vec (sort available))})))))
  (let [{:keys [perm bounds]} (group-perm dataset by within-order)
        base (ds/select-rows dataset perm)]
    (reduce (fn [d [col-kw dval]]
              (let [ast (deriv-ast dval)]
                (ds/add-column
                 d (ds/new-column col-kw
                                  (if (element-wise-ast? ast)
                                    (derive-column base col-kw dval)
                                    (window-derive base bounds col-kw dval))))))
            base
            (seq derivations))))

(defn- apply-group-set [dataset by derivations within-order]
  (if (zero? (ds/row-count dataset))
    dataset
    (if (and (keyword-only-by? by)
             (map? derivations)
             (every? #(deriv-ast (val %)) derivations))
      ;; Fast path: keyword-only :by + map derivations — reorder once, no concat.
      (fast-group-set dataset by derivations within-order)
      ;; General path. Compound case (qtile + exact keys): partition by exact keys
      ;; first so qtile breakpoints are computed per sub-dataset. Otherwise the
      ;; single "partition" is the whole dataset and the path is unchanged.
      (let [partitions (if (needs-per-partition-resolution? by)
                         (let [exact-keys (filterv keyword? by)]
                           (vals (ds/group-by dataset (fn [row] (select-keys row exact-keys)))))
                         [dataset])]
        (->> partitions
             (mapcat (fn [partition-ds]
                       (let [group-fn (by->group-fn partition-ds by)
                             groups (ds/group-by partition-ds group-fn)]
                         (map (fn [[_group-key sub-ds]]
                                (let [sorted (if within-order (apply-order-by sub-ds within-order :within-order) sub-ds)]
                                  (apply-set sorted derivations)))
                              groups))))
             (apply ds/concat))))))

(defn- apply-window-set
  "Window mode without :by — entire dataset is one partition.
  Optionally sorts by :within-order before applying derivations."
  [dataset derivations within-order]
  (let [sorted (if within-order (apply-order-by dataset within-order :within-order) dataset)]
    (apply-set sorted derivations)))

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
   (let [ms-per-unit (condp = unit
                       :seconds tech.v3.datatype.datetime/milliseconds-in-second
                       :minutes tech.v3.datatype.datetime/milliseconds-in-minute
                       :hours tech.v3.datatype.datetime/milliseconds-in-hour
                       :days tech.v3.datatype.datetime/milliseconds-in-day
                       :weeks tech.v3.datatype.datetime/milliseconds-in-week
                       (throw (ex-info (str "Unknown xbar temporal unit: " unit)
                                       {:dt/error :xbar-unknown-unit :unit unit})))]
     (with-meta
       (fn [row]
         (let [v (get row col-kw)]
           (when (some? v)
             (let [epoch-ms (tech.v3.datatype.datetime/datetime->epoch :epoch-milliseconds v)
                   epoch-units (Math/floorDiv ^long epoch-ms ^long ms-per-unit)]
               (* width (Math/floorDiv ^long epoch-units ^long width))))))
       {:xbar/col col-kw}))))

(defn qtile
  "Quantile bucketing — produces a :by grouping that bins each row's value
  in col-kw into one of n equal-count bins based on its percentile rank among
  non-nil values. Inspired by R's `cut` and Stata's `xtile`.

  Breakpoints are the 1/n, 2/n, ..., (n-1)/n quantiles (R type-7, matching
  cut-bucket). Each row is then assigned to a bin in [1, n] via
  left-inclusive comparison (values equal to a breakpoint go to the lower
  bin, matching cut-bucket). nil input values produce nil keys (their own
  group).

  Breakpoint population — depends on what else is in :by:
    * qtile alone in :by               → breakpoints from the WHOLE dataset
    * qtile + other exact keys in :by  → breakpoints are computed PER
                                         exact-key partition

  So `:by [:date (qtile :mktcap 5)]` does what you would expect in data.table
  or dplyr: each date's rows are binned against that date's own quintiles.
  This is the canonical CRSP / Fama-French pattern — per-date cross-sectional
  size quintiles.

  The optional :from keyword accepts a #dt/e boolean expression or a boolean
  column keyword selecting a reference subpopulation for breakpoint
  computation. When combined with other exact keys in :by, the mask is
  applied within each partition. Classic NYSE use case:

      (dt stocks :by [:date (qtile :mktcap 5 :from #dt/e (= :exchcd 1))]
          :agg {:mean-ret #dt/e (mn :ret)})

  per-date NYSE quintile breakpoints applied to all stocks (NYSE + AMEX +
  NASDAQ) — Fama-French size sort exactly.

  Companion to `xbar` (equal-width bins). For the same semantics inside
  #dt/e expressions (`:set` / `:where` / `:agg` contexts rather than :by),
  use `#dt/e (cut :col n :from pred)`.

  Result column name defaults to `<col>-q<n>` (e.g. :mktcap-q5 for quintile
  bins of :mktcap). Override via :datajure/col metadata on the marker.

  Note on small partitions: if a partition has fewer than n non-nil values,
  breakpoints cannot be computed and all non-nil rows in that partition
  land in bin 1. Consider filtering out thin partitions upstream or using
  fewer bins.

  Usage:
    ;; Global quintiles across the whole dataset
    (dt stocks :by [(qtile :mktcap 5)]
        :agg {:n N :mean-ret #dt/e (mn :ret)})

    ;; Per-date size quintiles — the canonical CRSP / Fama-French pattern
    (dt stocks :by [:date (qtile :mktcap 5)]
        :agg {:mean-ret #dt/e (mn :ret)})

    ;; Per-date NYSE quintile breakpoints applied to all stocks
    (dt stocks :by [:date (qtile :mktcap 5 :from #dt/e (= :exchcd 1))]
        :agg {:mean-ret #dt/e (mn :ret)})"
  [col-kw n & {:keys [from]}]
  (when-not (and (integer? n) (pos? n))
    (throw (ex-info (str "qtile requires a positive integer n, got: " n)
                    {:dt/error :qtile-invalid-n :n n})))
  (when-not (keyword? col-kw)
    (throw (ex-info (str "qtile requires a column keyword, got: " col-kw)
                    {:dt/error :qtile-invalid-col :col col-kw})))
  (cond-> {:dt/selector :qtile
           :dt/col col-kw
           :dt/n n
           :datajure/col (keyword (str (name col-kw) "-q" n))}
    (some? from) (assoc :dt/from from)))

(defn cut
  "Equal-count (quantile) binning — assigns each value in a column to a bin
  in 1..n based on its percentile rank among non-nil values.

  Breakpoints are the 1/n, 2/n, ..., (n-1)/n quantiles (R type-7) of the
  non-nil values. Bin assignment is right-open (binarySearch), so every
  value lands in exactly one bin in [1, n]. nil values produce nil.

  Complements xbar (equal-width bins). Use inside #dt/e:

    (dt ds :set {:quintile #dt/e (cut :mass 5)})
    (dt ds :where #dt/e (= (cut :mass 4) 1))   ;; bottom quartile

  Note: cut requires whole-column context and cannot be used as a standalone
  row-level function in :by. Use #dt/e (cut :col n) for all use cases."
  [col-kw n]
  (throw (ex-info "cut requires whole-column context — use inside #dt/e: (cut :col n)"
                  {:dt/error :cut-standalone-not-supported :col col-kw :n n})))

(defn between
  "Returns a column selector that selects all columns positionally between
  start-col and end-col (inclusive). Both endpoints must exist in the dataset.
  Intended for use with :select in dt.

  Example:
    (dt ds :select (between :month-01 :month-12))"
  [start-col end-col]
  {:dt/selector :between
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

(defn- normalise-order-spec [s]
  (if (keyword? s) {:order :asc :col s} s))

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

(defn- apply-order-by
  "Sort `dataset` by `specs` (per-key :asc/:desc). Reads only the sort-key columns
  and stable-sorts an index permutation with `clojure.core/compare` (nils first,
  mixed asc/desc), then gathers via `ds/select-rows`. Avoids tech's row-map
  `sort-by` path, which materialises a full row object per row even though only
  the key columns are compared — catastrophic for wide datasets. `context` labels
  validation errors (:order-by or :within-order)."
  [dataset specs context]
  (let [normalised (validate-order-specs dataset specs context)
        n          (ds/row-count dataset)]
    (if (or (< n 2) (empty? normalised))
      dataset
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
        (ds/select-rows dataset (sort cmp (range n)))))))

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
      (and (map? selector) (= :between (:dt/selector selector)))
      (let [{:dt/keys [start end]} selector
            _ (validate-select-cols dataset [start end])
            all-names (vec all-cols)
            si (.indexOf all-names start)
            ei (.indexOf all-names end)]
        (when (neg? si)
          (throw (ex-info (str "between: start column " start " not found")
                          {:dt/error :unknown-column :dt/columns #{start}})))
        (when (neg? ei)
          (throw (ex-info (str "between: end column " end " not found")
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

(defn dt
  "Query a dataset. Supported keywords: :where, :set, :agg, :by, :select, :order-by, :within-order, :take.

  :where         - filter rows. Accepts a #dt/e expression, a runtime data-form
                   vector (e.g. [:= :tic ticker] — keywords are columns, anything
                   else is a literal value, so runtime values flow in without a
                   row-map), or a plain fn of the row map.
  :set           - derive/update columns. Accepts map or vector-of-pairs.
                   When :set contains win/* functions, window mode is activated —
                   with :by, computes within groups; without :by, whole dataset is one partition.
  :agg           - collapse to summary. Accepts map or vector-of-pairs. Use N for row count.
  :by            - grouping for :agg or :set (partitioned window mode). Vector of keywords or fn of row.
  :within-order  - sort within each partition (or whole dataset) before :set or :agg runs.
                   Useful for window functions (win/lag, win/cumsum, ...) and for
                   order-sensitive aggregations (first-val, last-val, OHLC patterns).
                   With :set and :by: sorts within each group before window computation.
                   With :set and no :by: sorts whole dataset before window computation.
                   With :agg and :by: sorts within each group before aggregation.
                   With :agg and no :by: sorts whole dataset before aggregation.
  :select        - keep columns. Accepts: vector of kws, single kw, [:not kw ...],
                   regex, predicate fn, or map {old-kw new-kw} for rename-on-select.
  :order-by      - sort rows. Accepts a vector of (asc :col)/(desc :col) specs,
                   or bare keywords (default asc). Evaluated before :take.
  :take          - row limit (integer). Positive n keeps the first n rows (head),
                   negative keeps the last |n| (tail), 0 yields no rows. |n| beyond
                   the row count returns all rows. Evaluated last, after :order-by —
                   e.g. :order-by [(asc :date)] :take -20 is \"the last 20 by date\"."
  [dataset & {:keys [where set agg by select order-by within-order take]}]
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
  (let [set-has-win? (and set (derivations-have-win? set))
        window-mode? (and by set (not agg))]
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
      (info-note :window-mode "Window mode: computing within groups, keeping all rows.")
      (when (not within-order)
        (info-note :window-no-order "Window mode using current row order. Use :within-order to sort within groups.")))
    (when (and set-has-win? (not by))
      (info-note :window-mode-no-by "Window mode (whole dataset): computing over entire dataset, keeping all rows.")
      (when (not within-order)
        (info-note :window-no-order "Window mode using current row order. Use :within-order to sort.")))
    (cond-> dataset
      where (apply-where where)
      (and set by) (apply-group-set by set within-order)
      (and set (not by) (or within-order set-has-win?)) (apply-window-set set within-order)
      (and set (not by) (not within-order) (not set-has-win?)) (apply-set set)
      (and agg by) (apply-group-agg by agg within-order)
      (and agg (not by)) (apply-agg agg within-order)
      select (apply-select select)
      order-by (apply-order-by order-by :order-by)
      (some? take) (apply-take take))))
