(ns datajure.stat
  "Statistical transform functions for use inside #dt/e expressions.

  These functions operate on column vectors (dtype readers) and return a
  column of the same length. They are the runtime implementations for
  stat/* symbols parsed by datajure.expr.

  All functions are nil-safe: nil values in the input are skipped when
  computing reference statistics (mean, sd, percentiles), and nil inputs
  produce nil outputs in the returned column."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]))

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

(defn stat-winsorize
  "Winsorize a column at the given tail probability p (0 < p < 0.5).
  Values below the p-th percentile are clipped to that percentile;
  values above the (1-p)-th percentile are clipped to that percentile.
  nil values are preserved as nil.
  Example: (stat/winsorize :ret 0.01) clips the bottom and top 1%."
  [col p]
  (let [rdr (dtype/->reader col)
        n (dtype/ecount rdr)
        vals (filterv some? rdr)]
    (if (empty? vals)
      (dtype/make-reader :object n nil)
      (let [pct-lo (* (double p) 100.0)
            pct-hi (* (- 1.0 (double p)) 100.0)
            [lo hi] (dfn/percentiles vals [pct-lo pct-hi] {:nan-strategy :remove})
            lo (double lo)
            hi (double hi)]
        (dtype/make-reader :object n
                           (let [v (nth rdr idx)]
                             (when (some? v)
                               (let [d (double v)]
                                 (cond
                                   (< d lo) lo
                                   (> d hi) hi
                                   :else d)))))))))
