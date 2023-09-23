(ns datajure.operation-ck
  (:refer-clojure :exclude [group-by sort-by]))

(require '[clojask.dataframe :as ck]
         '[clojask.api.gb-aggregate :as gb-agg]
         '[clojask.api.aggregate :as agg]
         '[clojure.java.io :as io]
         '[clojask-io.output :as output])

(import '[com.google.code.externalsorting.csv CsvExternalSort CsvSortOptions$Builder]
        '[org.apache.commons.csv CSVFormat])

(defn ck-transform
  "Transform the structure of dataset from Datajure-style column-major associative map to Clojask-style row-major vector."
  [data]
  (into [] (cons (into [] (map name (keys data))) (apply map vector (vals data)))))

(def aggregate-function-keywords #{:min :mean :mode :max :sum :sd :skew :n-valid :n-missing :n})

(defn- filter-column-r
  "Perform `filter-operations` on `dataset`."
  [dataset filter-operations]
  (doall (map #(ck/filter dataset (name (first %)) (second %)) filter-operations))
  dataset)

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
        (reduce #(do (ck/filter %1 (first %2) (second %2)) %1) dataset (get query-map :row)))
      dataset)))

(defn- get-agg-name
  "Get the string representing the name of the aggregated column according to `col-name` and `agg-fun-keyword`."
  [col-name agg-fun-keyword]
  (str col-name "-" (name agg-fun-keyword)))

(defn- count-valid
  "Count the number of valid cells of `list`."
  [a b]
  (if (= a agg/start)
    (if (nil? b) 0 1)
    (+ a (if (nil? b) 0 1))))

(defn- count-missing
  "Count the number of missing cells of `list`."
  [a b]
  (if (= a agg/start)
    (if (nil? b) 1 0)
    (+ a (if (nil? b) 1 0))))

(defn- mean
  "Calculate the mean of `list`."
  [list]
  (let [sum (apply + list)
        count (count list)]
    (if (pos? count)
      (/ sum count)
      0)))

(defn- mode
  "Calculate the mode of `list`."
  [list]
  (let [freqs (frequencies list)
        occurrences (clojure.core/group-by val freqs)
        modes (last (sort occurrences))
        modes (->> modes
                   val
                   (map key))]
    modes))

(defn- median
  "Calculate the median of `list`."
  [list]
  (let [sorted (sort list)
        cnt (count sorted)
        halfway (quot cnt 2)]
    (if (odd? cnt)
      (nth sorted halfway)
      (let [bottom (dec halfway)
            bottom-val (nth sorted bottom)
            top-val (nth sorted halfway)]
        (mean [bottom-val top-val])))))

(defn- sd
  "Calculate the standard deviation of `list`."
  [list]
  (let [avg (mean list)
        squares (for [x list]
                  (let [x-avg (- x avg)]
                    (* x-avg x-avg)))
        total (count list)]
    (if (= 1 total)
      0
      (-> (/ (apply + squares)
             (- total 1))
          (Math/sqrt)))))

(defn- skew
  "Calculate the skew of `list`."
  [list]
  (let [mean (mean list)
        median (median list)
        sd (sd list)]
    (* 3 (/ (- mean median) sd))))

(def stat-ops
  {:min gb-agg/min
   :max gb-agg/max
   :sum gb-agg/sum
   :num-total gb-agg/count
   :num-valid count-valid
   :num-missing count-missing
   :mean mean
   :mode mode
   :skew skew
   :sd sd})

(defn- calc-stats
  "Calculate statistical information of `dataset` according to the operation represented by `mixed-words`."
  [dataset mixed-words]
  (if (empty? mixed-words)
    dataset
    (if (contains? aggregate-function-keywords (first mixed-words))
      (let
       [fst (first mixed-words)
        snd (first (rest mixed-words))]
        (ck/aggregate dataset (fst stat-ops) (name snd) (get-agg-name (name snd) fst))
        (calc-stats dataset (rest (rest mixed-words))))
      (calc-stats dataset (rest mixed-words)))))

(defn group-by
  "Group the records in `dataset` according to `query-map`."
  [dataset query-map]
  (let [group-by-col (get query-map :group-by)]
    (if (nil? group-by-col)
      dataset
      (if (or (seq? group-by-col) (list? group-by-col) (vector? group-by-col))
        (if (empty? group-by-col)
          dataset
          (ck/group-by dataset (map name group-by-col)))
        (ck/group-by dataset (name group-by-col)))))
  (calc-stats dataset (:select query-map))
  (ck/compute dataset 8 "./.dsl/group-by-result.csv")
  (ck/dataframe "./.dsl/group-by-result.csv"))

(defn having
  "Perform the `HAVING` operation on `dataset` by specifying a search condition for a group or an aggregate according to `query-map`."
  [dataset query-map]
  (let [unpharsed-filter-operations (get query-map :having)]
    (if (nil? unpharsed-filter-operations)
      dataset
      (filter-column-r dataset (mapv #(list (get-agg-name (name (second %)) (first %)) (last %)) unpharsed-filter-operations)))))

(defn- external-sort
  "Perform external sorting with comparator `comp` on the dataset stored in `input` and write the result into `output`."
  [input output comp]
  (let
   [input-file (io/as-file input)
    output-file (io/as-file output)
    temp-dir (io/as-file "./.dsl")]
    (.mkdir temp-dir)
    (let
     [csv-format (.withFirstRecordAsHeader CSVFormat/RFC4180)
      sort-option (let [builder (CsvSortOptions$Builder. comp CsvExternalSort/DEFAULTMAXTEMPFILES (CsvExternalSort/estimateAvailableMemory))]
                    (.numHeader builder 1)
                    (.skipHeader builder false)
                    (.format builder csv-format)
                    (.build builder))
      header (java.util.ArrayList.)
      file-list (CsvExternalSort/sortInBatch input-file temp-dir sort-option header)]
      (CsvExternalSort/mergeSortedFiles file-list output-file sort-option false header))))

(defn- cmp-gen
  "Generate comparator for the given `colname` and `compare-fn`."
  ([colname]
   #(- (Integer/parseInt (.get %1 colname))
       (Integer/parseInt (.get %2 colname))))
  ([colname compare-fn]
   #(compare-fn (Integer/parseInt (.get %1 colname))
                (Integer/parseInt (.get %2 colname)))))

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
              colname (if (contains? aggregate-function-keywords first-exp) (get-agg-name second-exp first-exp) (name first-exp))
              compare-fn (if (contains? aggregate-function-keywords first-exp) third-exp second-exp)
              input "./.dsl/sort-input.csv"
              output "./.dsl/sort-output.csv"]
          (with-open [writer (io/writer input :append false)]
            (output/write-csv writer (ck/compute dataset 8 nil) ","))
          (if (nil? compare-fn)
            (external-sort input output (cmp-gen colname))
            (external-sort input output (cmp-gen colname compare-fn)))
          (ck/dataframe output))))))

(defn- split-col-agg-keys-r
  "Convert aggregation keywords in `mixed-words` from separated form to combined form."
  [dataset mixed-words]
  (reduce #(if (contains? aggregate-function-keywords (last %1))
             (do
               (ck/aggregate dataset ((last %1) stat-ops) %2 (get-agg-name %2 (last %1)))
               (conj (into [] (butlast %1)) (get-agg-name %2 (last %1))))
             (conj %1 (name %2))) [] mixed-words))

(defn select
  "Select columns of `dataset` according to `query-map`."
  [dataset query-map]
  (let [select-all-keys (split-col-agg-keys-r dataset (:select query-map))]
     (if (empty? select-all-keys)
       (ck/compute dataset 8 "./.dsl/select-result.csv")
       (ck/compute dataset 8 "./.dsl/select-result.csv" :select select-all-keys)))
  dataset)
