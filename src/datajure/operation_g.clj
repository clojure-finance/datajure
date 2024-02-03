(ns datajure.operation-g
  (:refer-clojure :exclude [group-by sort-by]))

(require '[zero-one.geni.core :as g])

(def ^:private aggregate-function-keywords #{:min :mean :mode :max :sum :sd :skew :n-valid :n-missing :n})

(defn- filter-column-r
  "Perform `filter-operations` on `dataset`."
  [dataset filter-operations]
  (reduce #(g/filter %1 (get %2 1)) dataset filter-operations))

(defn where
  "Filter rows of `dataset` according to the given condition in `query-map`."
  [dataset query-map]
  (filter-column-r dataset (:where query-map)))

(defn row
  "Not supported. Please add an id column manually and use `filter` instead."
  [dataset _]
  dataset)

(defn- get-agg-key
  "Get the keyword representing the name of the aggregated column according to `col-name` and `agg-fun-keyword`."
  [col-name agg-fun-keyword]
  (let [agg-fun-name (if (= :sd agg-fun-keyword) "stddev_samp" (name agg-fun-keyword))]
    (keyword (str agg-fun-name "(" (name col-name) ")"))))

(def ^:private numeric-types ["ByteType" "ShortType" "IntegerType" "LongType" "FloatType" "DoubleType" "DecimalType"])

(defn- get-describe
  "Calculate statistical information of `grouped-dataset`."
  [grouped-dataset cols group-by-col]
  (let [numeric-cols (for [[k v] cols :when (and (some #{v} numeric-types) (not (some #{k} group-by-col)))] k)
        fns [g/count g/first g/max g/mean g/median g/min g/stddev g/sum]]
    (g/agg grouped-dataset (flatten (map #(map % numeric-cols) fns)))))

(defn group-by
  "Group the records in `dataset` according to `query-map`."
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
  "Perform the `HAVING` operation on `dataset` by specifying a search condition for a group or an aggregate according to `query-map`."
  [dataset query-map]
  (let [unpharsed-filter-operations (get query-map :having)]
    (if (nil? unpharsed-filter-operations)
      dataset
      (filter-column-r dataset (mapv #(vector (get-agg-key (second %) (first %)) (last %)) unpharsed-filter-operations)))))

(defn sort-by
  "Sort the records in `dataset` according to `query-map`."
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
  "Convert aggregation keywords in `mixed-words` from separated form to combined form."
  [mixed-words]
  (reduce #(if (contains? aggregate-function-keywords (last %1))
             (conj (into [] (butlast %1)) (get-agg-key %2 (last %1)))
             (conj %1 %2)) [] mixed-words))

(defn select
  "Select columns of `dataset` according to `query-map`."
  [dataset query-map]
  (let [select-all-keys (split-col-agg-keys-r (:select query-map))
        keys-with-first (map #(if (contains? (g/dtypes dataset) %) % (get-agg-key % :first)) select-all-keys)]
    (if (empty? keys-with-first) dataset (g/select dataset keys-with-first))))
