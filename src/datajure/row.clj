(ns datajure.row
  "Row-wise (cross-column) function implementations for datajure.
  Each function takes multiple columns and returns a single column
  of the same length. These operate across columns within a single row,
  complementing window functions (which operate down a single column).

  Nil conventions (matching spec):
    - row-sum: nil treated as 0 (like R rowSums(na.rm=TRUE))
    - row-mean, row-min, row-max: skip nil
    - All return nil when every input is nil"
  (:require [tech.v3.datatype :as dtype]))

(defn row-sum
  "Sum values across columns per row. Nil treated as 0; returns nil when all inputs are nil."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :float64 n
                       (let [vals (map #(nth % idx) readers)
                             non-nil (filter some? vals)]
                         (if (empty? non-nil)
                           nil
                           (reduce + 0.0 non-nil))))))

(defn row-mean
  "Mean of non-nil values across columns per row. Skips nil; returns nil when all inputs are nil."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :float64 n
                       (let [vals (map #(nth % idx) readers)
                             non-nil (filter some? vals)]
                         (if (empty? non-nil)
                           nil
                           (/ (reduce + 0.0 non-nil) (count non-nil)))))))

(defn row-min
  "Minimum of non-nil values across columns per row. Skips nil; returns nil when all inputs are nil."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :float64 n
                       (let [vals (map #(nth % idx) readers)
                             non-nil (filter some? vals)]
                         (if (empty? non-nil)
                           nil
                           (reduce min non-nil))))))

(defn row-max
  "Maximum of non-nil values across columns per row. Skips nil; returns nil when all inputs are nil."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :float64 n
                       (let [vals (map #(nth % idx) readers)
                             non-nil (filter some? vals)]
                         (if (empty? non-nil)
                           nil
                           (reduce max non-nil))))))

(defn row-count-nil
  "Count of nil values across columns per row. Returns an integer column."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :int64 n
                       (let [vals (map #(nth % idx) readers)]
                         (count (filter nil? vals))))))

(defn row-any-nil?
  "Boolean: true if any column value is nil for that row."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :boolean n
                       (let [vals (map #(nth % idx) readers)]
                         (boolean (some nil? vals))))))
