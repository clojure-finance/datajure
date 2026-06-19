(ns datajure.math
  "Low-level numeric primitives shared across datajure namespaces.

  Leaf namespace — depends only on tech.v3.datatype, so expr/core/stat/util can
  all use it without a require cycle (expr already requires stat, which rules out
  homing these in expr).

  Quantiles use R's default **type-7** estimator everywhere in datajure, chosen
  to match R's `quantile(..., type = 7)` / `median` exactly (the reference
  implementation for the finance workloads datajure is built against). This
  differs from tech.ml.dataset's `dfn/percentiles` / `dfn/median` (Apache Commons
  Math, a different estimation type) at the tails AND, for some n, the median.")

(defn- finite-sorted
  "Ascending double-array of the finite values in `coll`, dropping nil, NaN and
  ±Inf (R's `is.finite`). `coll` is any seqable of numbers (a dtype reader /
  column / vector / lazy seq) — reduced directly so a filtered lazy seq is fine."
  ^doubles [coll]
  (let [al (java.util.ArrayList.)]
    (run! (fn [v]
            (when (some? v)
              (let [d (double v)]
                (when-not (or (Double/isNaN d) (Double/isInfinite d))
                  (.add al d)))))
          coll)
    (let [m (.size al)
          arr (double-array m)]
      (dotimes [i m] (aset arr i (double (.get al i))))
      (java.util.Arrays/sort arr)
      arr)))

(defn quantile-type7
  "R type-7 quantile of the finite values in `coll` at probability `p` (a
  fraction in [0,1]).

  Drops nil and non-finite (NaN/±Inf) values, sorts ascending, and linearly
  interpolates at h = (n-1)p (0-indexed): for sorted x, lo = floor(h),
  result = x[lo] + (h-lo)·(x[lo+1] - x[lo]). Matches `quantile(x, p, type = 7,
  na.rm = TRUE)` in R.

  Returns nil when no finite values remain, or — when `min-n` is supplied — when
  fewer than `min-n` finite values remain (R's `if (length(x) < k) NA`). `min-n`
  is floor-free by default; pass it only for rules like the peer-bands n>=11."
  ([coll p] (quantile-type7 coll p nil))
  ([coll p min-n]
   (let [xs (finite-sorted coll)
         n (alength xs)]
     (cond
       (zero? n) nil
       (and min-n (< n (long min-n))) nil
       (= n 1) (aget xs 0)
       :else
       (let [pp (max 0.0 (min 1.0 (double p)))
             h (* (double (dec n)) pp)
             lo (min (long (Math/floor h)) (dec n))
             frac (- h lo)
             v (aget xs lo)]
         (if (>= (inc lo) n)
           v
           (+ v (* frac (- (aget xs (inc lo)) v)))))))))
