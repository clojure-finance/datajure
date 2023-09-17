(ns dp.ds-operation
  (:refer-clojure :exclude [group-by sort-by]))

(require '[tech.v3.dataset :as ds]
         '[tech.v3.dataset.join :as ds-join]
         '[clojure.algo.generic.functor :as gen])


(def aggregate-function-keywords #{:min :mean :mode :max :sum :sd :skew :n-valid :n-missing :n})

(defn- filter-column-r
  "Perform `filter-operations` on `dataset`."
  [dataset filter-operations]
  (reduce #(ds/filter-column %1 (first %2) (second %2)) dataset filter-operations))

(defn where
  "Filter rows of `dataset` according to the given condition in `query-map`."
  [dataset query-map]
  (filter-column-r dataset (:where query-map)))

(defn row
  "Select rows of `dataset` according to `query-map`."
  [dataset query-map]
  (let [where-val (get query-map :where)
        row-val (get query-map :row)]
    (if (or (nil? where-val) (empty? where-val))
      (if (or (= row-val :all) (empty? row-val))
        dataset
        (ds/select-rows-by-index dataset (get query-map :row)))
      dataset)))

(defn- get-agg-key
  "Get the keyword representing the name of the aggregated column according to `col-name` and `agg-fun-keyword`."
  [col-name agg-fun-keyword]
  (keyword (str (name col-name) "-" (name agg-fun-keyword))))

(defn- get-key-val
  "Get a vector containing the keyword and value. The keyword is generated according to `col-name` and `attr-keyword`."
  [col-name val attr-keyword]
  [(get-agg-key col-name attr-keyword) val])

(defn- get-description-column-ds
  "Convert the statistical information of the columns described by `groupby-col` and `groupby-col-val` from `descriptive-ds` to a dataset."
  [descriptive-ds groupby-col groupby-col-val]
  (let [list-col (descriptive-ds :col-name)
        num-col (get (meta list-col) :n-elems)

        list-min (descriptive-ds :min)
        list-mean (descriptive-ds :mean)
        list-mode (descriptive-ds :mode)
        list-max (descriptive-ds :max)
        list-sd (descriptive-ds :standard-deviation)
        list-skew (descriptive-ds :skew)
        list-num-valid (descriptive-ds :n-valid)
        list-num-missing (descriptive-ds :n-missing)
        list-num-total (mapv #(+ %1 %2) list-num-missing list-num-valid)
        list-sum (mapv #(if (and (number? %1) (number? %2)) (* %1 %2) nil) list-mean list-num-valid)


        min-keys (mapv #(get-key-val %1 %2 :min) list-col list-min)
        mean-keys (mapv #(get-key-val %1 %2 :mean) list-col list-mean)
        mode-keys (mapv #(get-key-val %1 %2 :mode) list-col list-mode)
        max-keys (mapv #(get-key-val %1 %2 :max) list-col list-max)
        sum-keys (mapv #(get-key-val %1 %2 :sum) list-col list-sum)
        sd-keys (mapv #(get-key-val %1 %2 :sd) list-col (mapv #(if (not (nil? %)) (if (Double/isNaN %) 0 %) %) list-sd))
        skew-keys (mapv #(get-key-val %1 %2 :skew) list-col list-skew)
        num-valid-keys (mapv #(get-key-val %1 %2 :n-valid) list-col list-num-valid)
        num-missing-keys (mapv #(get-key-val %1 %2 :n-missing) list-col list-num-missing)
        num-total-keys (mapv #(get-key-val %1 %2 :n) list-col list-num-total)]
    (ds/->dataset
     (into {} (reduce into [[[groupby-col groupby-col-val]] min-keys mean-keys mode-keys max-keys sum-keys sd-keys skew-keys num-valid-keys num-missing-keys num-total-keys])))))

(defn group-by-single
  "Perform `group-by` operation on `dataset` as specified by `group-by-col`."
  [dataset group-by-col]
  (if (nil? group-by-col)
    dataset
    (let [grouped-map (ds/group-by-column dataset group-by-col)
          descriptive-grouped-map (gen/fmap #(get-description-column-ds (ds/descriptive-stats %) group-by-col ((% group-by-col) 0)) grouped-map)
          fisrt-rows (mapv #(ds/select-rows-by-index % [0]) (vals grouped-map))
          first-ds (apply ds/concat fisrt-rows)
          agg-ds (apply ds/concat (vals descriptive-grouped-map))]
      (ds-join/left-join group-by-col first-ds agg-ds))))


(defn- get-combined-group-by-col
  "Get the combined form of column names as described by `group-by-col`."
  [group-by-col]
  (keyword (apply str (mapv name group-by-col))))

(defn- get-combined-group-by-val
  "Get the combined form of columns of `dataset` as described by `group-by-col`."
  [dataset group-by-col]
  (mapv #(apply str (str %)) (ds/value-reader (ds/select-columns dataset group-by-col))))

(defn group-by
  "Group the records in `dataset` according to `query-map`."
  [dataset query-map]
  (let [group-by-col (get query-map :group-by)]
    (if (nil? group-by-col)
      dataset
      (if (or (seq? group-by-col) (list? group-by-col) (vector? group-by-col))
        (if (empty? group-by-col)
          dataset
          (if (= 1 (count group-by-col))
            (group-by-single dataset (first group-by-col))
            (let [new-col (get-combined-group-by-col group-by-col)
                  new-dataset (assoc dataset new-col (get-combined-group-by-val dataset group-by-col))]
              (group-by-single new-dataset new-col))))
        (group-by-single dataset group-by-col)))))

(defn having
  "Perform the `HAVING` operation on `dataset` by specifying a search condition for a group or an aggregate according to `query-map`."
  [dataset query-map]
  (let [unpharsed-filter-operations (get query-map :having)]
    (if (nil? unpharsed-filter-operations)
      dataset
      (filter-column-r dataset (mapv #(list (get-agg-key (second %) (first %)) (last %)) unpharsed-filter-operations)))))

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
              third-exp (second (rest sort-by-expressions))
              colname (if (contains? aggregate-function-keywords first-exp) (get-agg-key second-exp first-exp) first-exp)
              compare-fn (if (contains? aggregate-function-keywords first-exp) third-exp second-exp)]
          (if (nil? compare-fn)
            (ds/sort-by-column dataset colname)
            (ds/sort-by-column dataset colname compare-fn)))))))


(defn- split-col-agg-keys-r
  "Convert aggregation keywords in `mixed-words` from separated form to combined form."
  [mixed-words]
  (reduce #(if (contains? aggregate-function-keywords (last %1))
             (conj (into [] (butlast %1)) (get-agg-key %2 (last %1)))
             (conj %1 %2)) [] mixed-words))

(defn select
  "Select columns of `dataset` according to `query-map`."
  [dataset query-map]
  (let [select-all-keys (split-col-agg-keys-r (:select query-map))]
    (ds/select-columns dataset (if (empty? select-all-keys) :all select-all-keys))))
