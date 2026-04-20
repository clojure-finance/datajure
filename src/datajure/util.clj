(ns datajure.util
  "Data cleaning and exploration utilities for datajure.
  Standalone functions that operate on datasets directly and thread naturally.
  Not part of dt — these complement it for common data preparation tasks."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.casting :as casting]
            [clojure.string :as str]))

(defn- describe-column [dataset col-kw]
  (let [col (dataset col-kw)
        dt (dtype/elemwise-datatype col)
        n (dtype/ecount col)
        n-missing (dtype/ecount (ds/missing col))
        numeric? (casting/numeric-type? dt)
        ;; Skip dfn stats if the column has no observable values (all-missing or
        ;; empty). Otherwise dfn/standard-deviation on an all-NaN :float64 column
        ;; returns -0.0 even though mean/min/max correctly return nil, yielding
        ;; an incoherent summary row.
        all-missing? (or (zero? n) (= n-missing n))]
    (if (and numeric? (not all-missing?))
      (let [pcts (dfn/percentiles col [25 50 75])]
        {:column col-kw
         :datatype dt
         :n n
         :n-missing n-missing
         :mean (dfn/mean col)
         :sd (dfn/standard-deviation col)
         :min (dfn/reduce-min col)
         :p25 (nth pcts 0)
         :median (nth pcts 1)
         :p75 (nth pcts 2)
         :max (dfn/reduce-max col)})
      {:column col-kw
       :datatype dt
       :n n
       :n-missing n-missing
       :mean nil :sd nil :min nil :p25 nil :median nil :p75 nil :max nil})))

(defn describe
  "Descriptive statistics for dataset columns. Returns a dataset with one row
  per column: :column, :datatype, :n, :n-missing, :mean, :sd, :min, :p25,
  :median, :p75, :max. Non-numeric columns show nil for stats.
  Optional second arg selects columns (vector of keywords)."
  ([dataset]
   (describe dataset (vec (ds/column-names dataset))))
  ([dataset cols]
   (let [cols (if (keyword? cols) [cols] cols)]
     (ds/->dataset (mapv #(describe-column dataset %) cols)))))

(defn clean-column-names
  "Clean column names: lowercase, replace any run of non-letter/non-digit characters
  with a single hyphen, collapse consecutive hyphens, strip leading/trailing hyphens.
  Unicode-aware: preserves letters and digits in all scripts (CJK, Cyrillic, Greek,
  accented Latin, etc). Only punctuation, whitespace, and symbols are replaced.

  \"Some Ugly Name!\" → :some-ugly-name
  \"市值 (HKD millions)!\" → :市值-hkd-millions"
  [dataset]
  (let [col-names (ds/column-names dataset)
        rename-map (into {}
                         (map (fn [col]
                                (let [clean (-> (name col)
                                                str/lower-case
                                                (str/replace #"[^\p{L}\p{N}]+" "-")
                                                (str/replace #"-+" "-")
                                                (str/replace #"^-|-$" ""))]
                                  [col (keyword clean)])))
                         col-names)]
    (ds/rename-columns dataset rename-map)))

(defn duplicate-rows
  "Returns dataset of duplicate rows only. Optional second arg specifies
  subset of columns to check for duplicates."
  ([dataset]
   (duplicate-rows dataset (vec (ds/column-names dataset))))
  ([dataset cols]
   (let [cols (if (keyword? cols) [cols] cols)
         grouped (group-by (fn [idx]
                             (mapv #(nth (dataset %) idx) cols))
                           (range (ds/row-count dataset)))
         dup-indices (->> grouped
                          vals
                          (filter #(> (count %) 1))
                          (mapcat identity)
                          sort
                          vec)]
     (ds/select-rows dataset dup-indices))))

(defn mark-duplicates
  "Adds :duplicate? boolean column. Optional second arg specifies
  subset of columns to check for duplicates."
  ([dataset]
   (mark-duplicates dataset (vec (ds/column-names dataset))))
  ([dataset cols]
   (let [cols (if (keyword? cols) [cols] cols)
         grouped (group-by (fn [idx]
                             (mapv #(nth (dataset %) idx) cols))
                           (range (ds/row-count dataset)))
         dup-set (into #{}
                       (mapcat identity)
                       (filter #(> (count %) 1) (vals grouped)))
         markers (mapv #(contains? dup-set %) (range (ds/row-count dataset)))]
     (ds/add-column dataset (ds/new-column :duplicate? markers)))))

(defn drop-constant-columns
  "Remove columns where all values are identical (zero variance).
  Note: columns with 0 or 1 rows are always kept — a single observation has no
  variance by definition, but that does not mean the column is constant across
  observations."
  [dataset]
  (let [keep-cols (filterv (fn [col-kw]
                             (let [col (dtype/->reader (dataset col-kw))
                                   n (dtype/ecount col)]
                               (if (< n 2)
                                 true
                                 (let [first-val (nth col 0)]
                                   (not (every? #(= first-val (nth col %))
                                                (range 1 n)))))))
                           (ds/column-names dataset))]
    (ds/select-columns dataset keep-cols)))

(defn coerce-columns
  "Bulk type coercion. col-type-map is {col-kw datatype-kw ...}.
  Example: (coerce-columns ds {:year :int64 :mass :float64})"
  [dataset col-type-map]
  (reduce-kv (fn [ds col-kw target-type]
               (ds/update-column ds col-kw #(dtype/elemwise-cast % target-type)))
             dataset
             col-type-map))
