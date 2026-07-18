(ns datajure.stat
  "Statistical transform functions for use inside #dt/e expressions.

  These functions operate on column vectors (dtype readers) and return a
  column of the same length. They are the runtime implementations for
  stat/* symbols parsed by datajure.expr.

  All functions are nil-safe: nil values in the input are skipped when
  computing reference statistics (mean, sd, percentiles), and nil inputs
  produce nil outputs in the returned column."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [datajure.math :as math]))

(defn stat-standardize
  "Standardize a column: (x - mean) / sd, element-wise.
  nil values are preserved as nil. Returns nil for entire column if sd is zero."
  [col]
  (let [rdr (dtype/->reader col)
        n (dtype/ecount rdr)
        vals (filterv some? rdr)
        mu (when (seq vals) (dfn/mean vals))
        sigma (when (seq vals) (dfn/standard-deviation vals))]
    (if (or (nil? mu) (nil? sigma) (zero? sigma))
      (dtype/make-reader :object n nil)
      (dtype/make-reader :object n
                         (let [v (nth rdr idx)]
                           (when (some? v) (/ (- (double v) mu) sigma)))))))

(defn stat-demean
  "Demean a column: x - mean(x), element-wise.
  nil values are preserved as nil."
  [col]
  (let [rdr (dtype/->reader col)
        n (dtype/ecount rdr)
        vals (filterv some? rdr)
        mu (when (seq vals) (dfn/mean vals))]
    (if (nil? mu)
      (dtype/make-reader :object n nil)
      (dtype/make-reader :object n
                         (let [v (nth rdr idx)]
                           (when (some? v) (- (double v) mu)))))))

(defn- check-tail!
  "Validate a `:tail` option (shared by winsorize and any future tail-aware
  transform). Returns the tail keyword (default :both)."
  [fn-name opts]
  (let [tail (:tail opts :both)]
    (if (contains? #{:both :lower :upper} tail)
      tail
      (throw (ex-info (str fn-name ": unknown :tail " (pr-str tail)
                           ". Supported: :both :lower :upper.")
                      {:dt/error :invalid-tail :dt/tail tail})))))

(defn stat-winsorize
  "Winsorize a column at the given tail probability p (0 < p < 0.5).
  Values below the p-th percentile are clipped to that percentile;
  values above the (1-p)-th percentile are clipped to that percentile.
  nil values are preserved as nil.

  An optional trailing options map restricts clipping to one tail
  (mbmisc winsor.bm / winsor.tp): {:tail :lower} clips only the bottom p,
  {:tail :upper} only the top p; the default is {:tail :both}.

  Examples: (stat/winsorize :ret 0.01) clips the bottom and top 1%;
  (stat/winsorize :ret 0.01 {:tail :upper}) clips only the top 1%."
  ([col p] (stat-winsorize col p nil))
  ([col p opts]
   (let [tail (check-tail! "stat/winsorize" opts)
         rdr (dtype/->reader col)
         n (dtype/ecount rdr)
         vals (filterv some? rdr)]
     (if (empty? vals)
       (dtype/make-reader :object n nil)
       (let [lo (when (contains? #{:both :lower} tail)
                  (double (math/quantile-type7 vals (double p))))
             hi (when (contains? #{:both :upper} tail)
                  (double (math/quantile-type7 vals (- 1.0 (double p)))))]
         (dtype/make-reader :object n
                            (let [v (nth rdr idx)]
                              (when (some? v)
                                (let [d (double v)]
                                  (cond
                                    (and lo (< d lo)) lo
                                    (and hi (> d hi)) hi
                                    :else d))))))))))

(defn stat-trim
  "Trim a column at the given tail probability p (0 < p < 0.5): values below
  the p-th percentile or above the (1-p)-th percentile become nil (removed,
  not clipped — the trimming counterpart of [[stat-winsorize]]). Values equal
  to a percentile bound are kept. nil values stay nil; NaN/±Inf compare false
  against the bounds and are trimmed to nil.
  Example: (stat/trim :ret 0.01) nils out the bottom and top 1%."
  [col p]
  (let [rdr (dtype/->reader col)
        n (dtype/ecount rdr)
        vals (filterv some? rdr)]
    (if (empty? vals)
      (dtype/make-reader :object n nil)
      (let [lo (double (math/quantile-type7 vals (double p)))
            hi (double (math/quantile-type7 vals (- 1.0 (double p))))]
        (dtype/make-reader :object n
                           (let [v (nth rdr idx)]
                             (when (some? v)
                               (let [d (double v)]
                                 (when (and (>= d lo) (<= d hi))
                                   d)))))))))

(defn stat-rescale
  "Linearly rescale a column to the range [new-lo, new-hi] (min-max scaling;
  default [0.0, 1.0]): new-lo + (x - min) / (max - min) * (new-hi - new-lo).
  min/max are taken over the column's non-nil values. nil values are preserved
  as nil. A constant column (max = min) has no defined scale and returns all
  nil, matching standardize's zero-sd behavior.
  Examples: (stat/rescale :x) → [0,1]; (stat/rescale :x -1 1) → [-1,1]."
  ([col] (stat-rescale col 0.0 1.0))
  ([col new-lo new-hi]
   (let [rdr (dtype/->reader col)
         n (dtype/ecount rdr)
         vals (filterv some? rdr)
         old-lo (when (seq vals) (double (reduce min vals)))
         old-hi (when (seq vals) (double (reduce max vals)))]
     (if (or (nil? old-lo) (= old-lo old-hi))
       (dtype/make-reader :object n nil)
       (let [scale (/ (- (double new-hi) (double new-lo)) (- old-hi old-lo))
             lo (double new-lo)]
         (dtype/make-reader :object n
                            (let [v (nth rdr idx)]
                              (when (some? v)
                                (+ lo (* scale (- (double v) old-lo)))))))))))
