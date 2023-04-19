(ns dp.ds_operation
  (:refer-clojure :exclude [group-by sort-by]))

(require '[tech.v3.dataset :as ds]
         '[tech.v3.datatype :as dtype]
         '[tech.v3.dataset.join :as ds-join]
         '[clojure.algo.generic.functor :as gen])


(def aggregate-function-keywords #{:min :mean :mode :max :sum :sd :skew :n-valid :n-missing :n})


(defn- filter-column-r
  [dataset filter-operations]
  (if (empty? filter-operations)
    dataset
    (let [cur-filter (first filter-operations)]
      (filter-column-r (ds/filter-column dataset (first cur-filter) (second cur-filter)) (rest filter-operations))
      )
    ))

(defn where
  [dataset query-map]
  (let [filter-operations (get query-map :where)]
    (if (nil? filter-operations)
      dataset
      (if (= filter-operations [:*])
        dataset
        (filter-column-r dataset filter-operations)))))

(defn row
  [dataset query-map]
  (let [where-val (get query-map :where)
        row-val (get query-map :row)]
    (if (or (nil? where-val) (empty? where-val))
      (if (or (= row-val :all) (empty? row-val))
        dataset
        (ds/select-rows-by-index dataset (get query-map :row)))
      dataset)))

(defn- get-agg-key
  [col-name agg-fun-keyword]
  (keyword (str (name col-name) "-" (name agg-fun-keyword))))

(defn- get-key-val
  [col-name val attr-keyword]
  [(get-agg-key col-name attr-keyword) val])

(defn- get-description-column-ds
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
    ;(println list-sd)
    (ds/->dataset
     (into {} (reduce into [[[groupby-col groupby-col-val]] min-keys mean-keys mode-keys max-keys sum-keys sd-keys skew-keys num-valid-keys num-missing-keys num-total-keys])))))

(defn group-by-single
  [dataset group-by-col]
    (if (nil? group-by-col)
      dataset
      (let [grouped-map (ds/group-by-column dataset group-by-col)
            descriptive-grouped-map (gen/fmap #(get-description-column-ds (ds/descriptive-stats %) group-by-col ((% group-by-col) 0)) grouped-map)
            fisrt-rows (mapv #(ds/select-rows-by-index % [0]) (vals grouped-map))
            first-ds (apply ds/concat fisrt-rows)
            agg-ds (apply ds/concat (vals descriptive-grouped-map))]
        ;(println "First DS" first-ds)
        (ds-join/left-join group-by-col first-ds agg-ds))))


(defn- get-combined-group-by-col
  [group-by-col]
  (keyword (apply str (mapv name group-by-col))))

(defn- get-combined-group-by-val
  [dataset group-by-col]
  (mapv #(apply str %) (ds/value-reader (ds/select-columns dataset group-by-col))))


(defn group-by
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
              ;(println new-dataset)
              (group-by-single new-dataset new-col))))
        (group-by-single dataset group-by-col)))))
    

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
              third-exp (second (rest sort-by-expressions))
              colname (if (contains? aggregate-function-keywords first-exp) (get-agg-key second-exp first-exp) first-exp)
              compare-fn (if (contains? aggregate-function-keywords first-exp) third-exp second-exp)]
          (if (nil? compare-fn)
            (ds/sort-by-column dataset colname)
            (ds/sort-by-column dataset colname compare-fn)))))))



(defn- split-col-agg-keys-r
  [keys mixed-list original-col]
  (let [first-word (first mixed-list)
        second-word (second mixed-list)]
    (if (nil? first-word)
      keys
      (if (contains? aggregate-function-keywords first-word)
        (split-col-agg-keys-r (conj keys (get-agg-key second-word first-word)) (rest (rest mixed-list)) original-col)
        (if (= first-word :*)
         (split-col-agg-keys-r (into [] (concat keys original-col)) (rest mixed-list) original-col)
         (split-col-agg-keys-r (conj keys first-word) (rest mixed-list) original-col))))))

        

(defn select
  [dataset query-map]
  (let
   [original-col (get query-map :original-col)
    select-all-keys (split-col-agg-keys-r [] (get query-map :select) original-col)
    select-all-keys (into [] (distinct select-all-keys))]
    (ds/select-columns dataset select-all-keys)))

