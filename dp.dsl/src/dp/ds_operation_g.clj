(ns dp.ds-operation-g
  (:refer-clojure :exclude [group-by sort-by]))

(require '[zero-one.geni.core :as g])

(def aggregate-function-keywords #{:min :mean :mode :max :sum :sd :skew :n-valid :n-missing :n})

(defn- filter-column-r
  [dataset filter-operations]
  (reduce #(g/filter %1 %2) dataset filter-operations))

(defn where
  [dataset query-map]
  (filter-column-r dataset (:where query-map)))

;; Not supported.
;; Please add an id column manually and use `filter` instead.
(defn row [dataset _] dataset)

(defn- get-agg-key
  [col-name agg-fun-keyword]
  (let [agg-fun-name (if (= :sd agg-fun-keyword) "stddev_samp" (name agg-fun-keyword))]
    (keyword (str agg-fun-name "(" (name col-name) ")"))))

(def numeric-types ["ByteType" "ShortType" "IntegerType" "LongType" "FloatType" "DoubleType" "DecimalType"])

(defn- get-describe
  [grouped-dataset cols group-by-col]
  (let [numeric-cols (for [[k v] cols :when (and (some #{v} numeric-types) (not (some #{k} group-by-col)))] k)
        fns [g/count g/first g/max g/mean g/median g/min g/stddev g/sum]]
    (g/agg grouped-dataset (flatten (map #(map % numeric-cols) fns)))))

(defn group-by
  [dataset query-map]
  (let [group-by-col (get query-map :group-by)]
    (if (nil? group-by-col)
      dataset
      (if (or (seq? group-by-col) (list? group-by-col) (vector? group-by-col))
        (if (empty? group-by-col)
          dataset
          (get-describe (g/group-by dataset group-by-col) (g/dtypes dataset) group-by-col))
        (get-describe (g/group-by dataset group-by-col) (g/dtypes dataset) group-by-col)))))

(defn having
  [dataset query-map]
  (let [unpharsed-filter-operations (get query-map :having)]
    (if (nil? unpharsed-filter-operations)
      dataset
      (filter-column-r dataset (mapv #(list (get-agg-key (second %) (first %)) (last %)) unpharsed-filter-operations)))))

(defn sort-by
  [dataset query-map]
  (let [sort-by-expressions (get query-map :sort-by)]
    (if (nil? sort-by-expressions)
      dataset
      (if (empty? sort-by-expressions)
        dataset
        (let [first-exp (first sort-by-expressions)
              second-exp (second sort-by-expressions)
              colname (if (contains? aggregate-function-keywords first-exp)
                        (get-agg-key second-exp first-exp)
                        (if (contains? (g/dtypes dataset) first-exp)
                          first-exp
                          (get-agg-key first-exp :first)))]
          (g/sort dataset colname))))))


(defn- split-col-agg-keys-r
  [mixed-words]
  (reduce #(if (contains? aggregate-function-keywords (last %1))
             (conj (into [] (butlast %1)) (get-agg-key %2 (last %1)))
             (conj %1 %2)) [] mixed-words))

(defn select
  [dataset query-map]
  (let [select-all-keys (split-col-agg-keys-r (:select query-map))
        keys-with-first (map #(if (contains? (g/dtypes dataset) %) % (get-agg-key % :first)) select-all-keys)]
    (if (empty? keys-with-first) dataset (g/select dataset keys-with-first))))
