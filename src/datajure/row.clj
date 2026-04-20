(ns datajure.row
  "Row-wise (cross-column) function implementations for datajure.
  Each function takes multiple columns and returns a single column
  of the same length. These operate across columns within a single row,
  complementing window functions (which operate down a single column).

  Nil conventions (matching spec):
    - row-sum: nil treated as 0 (like R rowSums(na.rm=TRUE))
    - row-mean, row-min, row-max: skip nil
    - All return nil when every input is nil

  Return dtypes:
    - row-sum / row-mean / row-min / row-max return :object readers so that
      the all-nil-row case can return nil honestly. A :float64 reader would
      coerce nil to NaN (which tech.v3.dataset then folds into the missing
      set when stored in a dataset column — so dt-wrapped use looks fine,
      but a direct caller reading the reader would see NaN, contradicting
      the docstring).
    - row-count-nil returns :int64 (always a count, never nil).
    - row-any-nil? returns :boolean (always a boolean, never nil)."
  (:require [tech.v3.datatype :as dtype]))

(defn row-sum
  "Sum values across columns per row. Nil treated as 0; returns nil when all inputs are nil."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :object n
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
    (dtype/make-reader :object n
                       (let [vals (map #(nth % idx) readers)
                             non-nil (filter some? vals)]
                         (if (empty? non-nil)
                           nil
                           (/ (reduce + 0.0 non-nil) (count non-nil)))))))

(defn row-min
  "Minimum of non-nil values across columns per row. Skips nil; returns nil when all inputs are nil.
  Non-nil results are promoted to double for consistency with row-sum and row-mean."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :object n
                       (let [vals (map #(nth % idx) readers)
                             non-nil (filter some? vals)]
                         (if (empty? non-nil)
                           nil
                           (double (reduce min non-nil)))))))

(defn row-max
  "Maximum of non-nil values across columns per row. Skips nil; returns nil when all inputs are nil.
  Non-nil results are promoted to double for consistency with row-sum and row-mean."
  [& cols]
  (let [n (dtype/ecount (first cols))
        readers (mapv dtype/->reader cols)]
    (dtype/make-reader :object n
                       (let [vals (map #(nth % idx) readers)
                             non-nil (filter some? vals)]
                         (if (empty? non-nil)
                           nil
                           (double (reduce max non-nil)))))))

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
