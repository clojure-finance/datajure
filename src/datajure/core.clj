(ns datajure.core
  (:require [tech.v3.dataset :as ds]
            [datajure.expr :as expr])
  (:import [java.util Comparator]))

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
      (throw (ex-info (str "Unknown column(s) " unknown " in " context " expression")
                      {:dt/error :unknown-column
                       :dt/columns unknown
                       :dt/context context
                       :dt/available (vec (sort available))})))))

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

(defn- apply-group-agg [dataset by-cols aggregations]
  (let [pairs (if (map? aggregations) (seq aggregations) aggregations)
        groups (ds/group-by dataset (fn [row] (select-keys row by-cols)))]
    (->> groups
         (map (fn [[group-key sub-ds]]
                (let [wrapped-key (update-vals group-key vector)
                      agg-result (reduce (fn [m [col-kw agg-fn]]
                                           (assoc m col-kw [(eval-agg sub-ds col-kw agg-fn)]))
                                         wrapped-key
                                         pairs)]
                  (ds/->dataset agg-result))))
         (apply ds/concat))))

(defn- apply-group-set [dataset by-cols derivations]
  (let [groups (ds/group-by dataset (fn [row] (select-keys row by-cols)))]
    (->> groups
         (map (fn [[_group-key sub-ds]] (apply-set sub-ds derivations)))
         (apply ds/concat))))

(def N
  "Row count aggregation helper. Use as a value in :agg maps."
  ds/row-count)

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

(defn- apply-select [dataset selector]
  (let [all-cols (ds/column-names dataset)]
    (cond
      (map? selector)
      (-> dataset
          (ds/select-columns (keys selector))
          (ds/rename-columns selector))

      (and (vector? selector) (= :not (first selector)))
      (let [excluded (set (rest selector))]
        (ds/select-columns dataset (remove excluded all-cols)))

      (vector? selector)
      (ds/select-columns dataset selector)

      (keyword? selector)
      (ds/select-columns dataset [selector])

      (instance? java.util.regex.Pattern selector)
      (ds/select-columns dataset (filter #(re-find selector (name %)) all-cols))

      (fn? selector)
      (ds/select-columns dataset (filter selector all-cols))

      :else
      (throw (ex-info "Invalid :select argument" {:selector selector})))))

(defn dt
  "Query a dataset. Supported keywords: :where, :set, :agg, :by, :select, :order-by.

  :where    - filter rows. Accepts #dt/e expression or plain fn of row map.
  :set      - derive/update columns. Accepts map or vector-of-pairs.
  :agg      - collapse to summary. Accepts map or vector-of-pairs. Use N for row count.
  :by       - group columns for :agg or :set (window mode). Vector of keywords.
  :select   - keep columns. Accepts: vector of kws, single kw, [:not kw ...],
              regex, predicate fn, or map {old-kw new-kw} for rename-on-select.
  :order-by - sort rows. Accepts a vector of (asc :col)/(desc :col) specs,
              or bare keywords (default asc). Evaluated after all other steps."
  [dataset & {:keys [where set agg by select order-by]}]
  (when (and set agg)
    (throw (ex-info "Cannot combine :set and :agg in the same dt call. Use -> threading for multi-step queries."
                    {:dt/error :set-agg-conflict})))
  (cond-> dataset
    where (apply-where where)
    (and set by) (apply-group-set by set)
    (and set (not by)) (apply-set set)
    (and agg by) (apply-group-agg by agg)
    (and agg (not by)) (apply-agg agg)
    select (apply-select select)
    order-by (apply-order-by order-by)))
