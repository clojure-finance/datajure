(ns datajure.core
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.casting :as casting]
            [clojure.set :as set]
            [datajure.expr :as expr])
  (:import [java.util Comparator]))

(declare apply-order-by)

(defn- levenshtein [s t]
  (let [s (vec s) t (vec t)
        m (count s) n (count t)
        row (vec (range (inc n)))]
    (loop [i 0 row row]
      (if (= i m)
        (row n)
        (recur (inc i)
               (loop [j 0 prev (inc i) row row]
                 (if (= j n)
                   row
                   (let [cost (if (= (s i) (t j)) 0 1)
                         v (min (inc (row (inc j)))
                                (inc prev)
                                (+ (row j) cost))]
                     (recur (inc j) (row (inc j)) (assoc row (inc j) v))))))))))

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
                                                        (map (fn [a] [a (levenshtein col-str (name a))]))
                                                        (sort-by second)
                                                        first)]
                                       [col (when (<= (second closest) 3) [(first closest)])])))
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
  (if (expr-node? predicate)
    (do (validate-expr-cols dataset predicate :where)
        (ds/select-rows dataset ((expr/compile-expr predicate) dataset)))
    (ds/filter dataset predicate)))

(defn- derive-column [dataset col-kw col-fn]
  (if (expr-node? col-fn)
    (do (validate-expr-cols dataset col-fn (str ":set " col-kw))
        ((expr/compile-expr col-fn) dataset))
    (mapv col-fn (ds/mapseq-reader dataset))))

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

(defn- eval-agg [dataset col-kw agg-fn]
  (if (expr-node? agg-fn)
    (do (validate-expr-cols dataset agg-fn (str ":agg " col-kw))
        ((expr/compile-expr agg-fn) dataset))
    (agg-fn dataset)))

(defn- apply-agg [dataset aggregations]
  (let [pairs (if (map? aggregations) (seq aggregations) aggregations)
        result (reduce (fn [m [col-kw agg-fn]]
                         (assoc m col-kw [(eval-agg dataset col-kw agg-fn)]))
                       {}
                       pairs)]
    (ds/->dataset result)))

(defn- by->group-fn [by]
  (cond
    (fn? by)
    by
    (every? keyword? by)
    (fn [row] (select-keys row by))
    :else
    (fn [row]
      (into {}
            (map-indexed (fn [i item]
                           (if (keyword? item)
                             [item (get row item)]
                             [(or (-> item meta :xbar/col) (keyword (str "xbar-" i))) (item row)]))
                         by)))))

(defn- apply-group-agg [dataset by aggregations]
  (when (pos? (ds/row-count dataset))
    (let [pairs (if (map? aggregations) (seq aggregations) aggregations)
          group-fn (by->group-fn by)
          groups (ds/group-by dataset group-fn)]
      (->> groups
           (map (fn [[group-key sub-ds]]
                  (let [wrapped-key (update-vals group-key vector)
                        agg-result (reduce (fn [m [col-kw agg-fn]]
                                             (assoc m col-kw [(eval-agg sub-ds col-kw agg-fn)]))
                                           wrapped-key
                                           pairs)]
                    (ds/->dataset agg-result))))
           (apply ds/concat)))))

(defn- apply-group-set [dataset by derivations within-order]
  (when (pos? (ds/row-count dataset))
    (let [group-fn (by->group-fn by)
          groups (ds/group-by dataset group-fn)]
      (->> groups
           (map (fn [[_group-key sub-ds]]
                  (let [sorted (if within-order (apply-order-by sub-ds within-order) sub-ds)]
                    (apply-set sorted derivations))))
           (apply ds/concat)))))

(defn- apply-window-set
  "Window mode without :by — entire dataset is one partition.
  Optionally sorts by :within-order before applying derivations."
  [dataset derivations within-order]
  (let [sorted (if within-order (apply-order-by dataset within-order) dataset)]
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
  "Row count aggregation helper. Use as a value in :agg maps."
  ds/row-count)

(def mean
  "Column mean. Full-name alias for `dfn/mean`."
  dfn/mean)

(def sum
  "Column sum. Full-name alias for `dfn/sum`."
  dfn/sum)

(def median
  "Column median. Full-name alias for `dfn/median`."
  dfn/median)

(def stddev
  "Column standard deviation. Full-name alias for `dfn/standard-deviation`."
  dfn/standard-deviation)

(def variance
  "Column variance. Full-name alias for `dfn/variance`."
  dfn/variance)

(def max*
  "Column maximum. Full-name alias for `dfn/reduce-max`.
  Asterisk-suffixed to avoid shadowing `clojure.core/max`."
  dfn/reduce-max)

(def min*
  "Column minimum. Full-name alias for `dfn/reduce-min`.
  Asterisk-suffixed to avoid shadowing `clojure.core/min`."
  dfn/reduce-min)

(defn count*
  "Count of non-nil values in a column.
  Asterisk-suffixed to avoid shadowing `clojure.core/count`.
  Distinct from N (total rows) and count-distinct (unique non-nil values)."
  [col]
  (count (remove nil? (dtype/->reader col))))

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
           (* width (quot v width)))))
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
                   epoch-units (quot epoch-ms ms-per-unit)]
               (* width (quot epoch-units width))))))
       {:xbar/col col-kw}))))

(defn cut
  "Equal-count (quantile) binning — assigns each value in a column to a bin
  in 1..n based on its percentile rank among non-nil values.

  Breakpoints are the 100/n, 200/n, ..., (n-1)*100/n percentiles of the
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

(defn- specs->comparator [specs]
  (reify Comparator
    (compare [_ row-a row-b]
      (reduce (fn [_ {:keys [order col]}]
                (let [c (clojure.core/compare (get row-a col) (get row-b col))
                      result (if (= order :desc) (- c) c)]
                  (if (not= result 0) (reduced result) 0)))
              0
              specs))))

(defn- apply-order-by [dataset specs]
  (let [normalised (map normalise-order-spec specs)]
    (ds/sort-by dataset identity (specs->comparator normalised))))

(defn- validate-select-cols
  "Checks that all requested column keywords exist in the dataset.
  Throws ex-info with Levenshtein suggestions on unknown columns."
  [dataset requested]
  (let [available (set (ds/column-names dataset))
        unknown (set/difference (set requested) available)]
    (when (seq unknown)
      (let [avail-names (vec (sort available))
            suggestions (into {}
                              (map (fn [col]
                                     (let [col-str (name col)
                                           closest (->> avail-names
                                                        (map (fn [a] [a (levenshtein col-str (name a))]))
                                                        (sort-by second)
                                                        first)]
                                       [col (when (<= (second closest) 3) [(first closest)])])))
                              unknown)]
        (throw (ex-info (str "Unknown column(s) " unknown " in :select")
                        {:dt/error :unknown-column
                         :dt/columns unknown
                         :dt/context :select
                         :dt/available avail-names
                         :dt/closest suggestions}))))))

(defn- apply-select [dataset selector]
  (let [all-cols (ds/column-names dataset)
        col-dtype (fn [col-kw]
                    (-> (ds/column dataset col-kw) meta :datatype))]
    (cond
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
  "Query a dataset. Supported keywords: :where, :set, :agg, :by, :select, :order-by, :within-order.

  :where         - filter rows. Accepts #dt/e expression or plain fn of row map.
  :set           - derive/update columns. Accepts map or vector-of-pairs.
                   When :set contains win/* functions, window mode is activated —
                   with :by, computes within groups; without :by, whole dataset is one partition.
  :agg           - collapse to summary. Accepts map or vector-of-pairs. Use N for row count.
  :by            - grouping for :agg or :set (partitioned window mode). Vector of keywords or fn of row.
  :within-order  - sort within each partition (or whole dataset) before window computation.
                   Valid with :set (with or without :by). Not valid with :agg.
  :select        - keep columns. Accepts: vector of kws, single kw, [:not kw ...],
                   regex, predicate fn, or map {old-kw new-kw} for rename-on-select.
  :order-by      - sort rows. Accepts a vector of (asc :col)/(desc :col) specs,
                   or bare keywords (default asc). Evaluated after all other steps."
  [dataset & {:keys [where set agg by select order-by within-order]}]
  (when (and set agg)
    (throw (ex-info "Cannot combine :set and :agg in the same dt call. Use -> threading for multi-step queries."
                    {:dt/error :set-agg-conflict})))
  (when (and within-order agg)
    (throw (ex-info ":within-order is not valid with :agg."
                    {:dt/error :within-order-invalid})))
  (when (and within-order (not set))
    (throw (ex-info ":within-order requires :set."
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
      (and agg by) (apply-group-agg by agg)
      (and agg (not by)) (apply-agg agg)
      select (apply-select select)
      order-by (apply-order-by order-by))))
