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

(defn finite-double?
  "True for a non-nil, finite number — not nil, NaN, or ±Inf (R's `is.finite`)."
  [x]
  (and (some? x)
       (let [d (double x)]
         (and (not (Double/isNaN d)) (not (Double/isInfinite d))))))

(defn asinh
  "Numerically-stable inverse hyperbolic sine: sign(x)·ln(|x| + sqrt(x²+1)).
  Returns nil for nil or non-finite input. The textbook ln(x + sqrt(x²+1)) form
  suffers catastrophic cancellation for large negative x (x + sqrt(x²+1) → 0 →
  ln → -Inf), which silently turns legitimate large-negative growth into nil;
  computing on |x| and reapplying the sign is stable everywhere."
  [x]
  (when (finite-double? x)
    (let [d (double x)
          a (Math/abs d)
          r (Math/log (+ a (Math/sqrt (+ (* a a) 1.0))))]
      (if (neg? d) (- r) r))))

(defn- finite-sorted
  "Ascending double-array of the finite values in `coll`, dropping nil, NaN and
  ±Inf (R's `is.finite`). `coll` is any seqable of numbers (a dtype reader /
  column / vector / lazy seq) — reduced directly so a filtered lazy seq is fine.

  A non-nil, non-numeric value (e.g. a java.time date — quantiles can't rank one)
  throws a structured `:quantile-non-numeric` error rather than a raw
  ClassCastException deep in the cast."
  ^doubles [coll]
  (let [al (java.util.ArrayList.)]
    (run! (fn [v]
            (when (some? v)
              (if (number? v)
                (let [d (double v)]
                  (when-not (or (Double/isNaN d) (Double/isInfinite d))
                    (.add al d)))
                (throw (ex-info
                        (str "quantile/median requires a numeric column; got a "
                             (.getName (class v)) " (" (pr-str v) "). Convert a "
                             "date/temporal column to epoch days or millis first.")
                        {:dt/error :quantile-non-numeric
                         :dt/value-class (.getName (class v))})))))
          coll)
    (let [m (.size al)
          arr (double-array m)]
      (dotimes [i m] (aset arr i (double (.get al i))))
      (java.util.Arrays/sort arr)
      arr)))

(defn- quantile-of-sorted
  "Type-7 quantile at probability `p` of the pre-sorted finite double-array `xs`
  (length `n`). nil when empty or below the `min-n` floor. Shared by the single-
  and multi-probability entry points so a column is sorted at most once."
  [^doubles xs ^long n p min-n]
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
        (+ v (* frac (- (aget xs (inc lo)) v)))))))

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
   (let [xs (finite-sorted coll)]
     (quantile-of-sorted xs (alength xs) p min-n))))

(defn quantiles-type7
  "Type-7 quantiles at several probabilities `ps`, sorting the finite values of
  `coll` **once**. Returns a vector aligned with `ps` (each element nil under the
  same empty / `min-n` rules as `quantile-type7`). This is the efficient form for
  the q20/median/q80 band idiom — one sort instead of three."
  ([coll ps] (quantiles-type7 coll ps nil))
  ([coll ps min-n]
   (let [xs (finite-sorted coll)
         n (alength xs)]
     (mapv #(quantile-of-sorted xs n % min-n) ps))))

(defn quantiles-of-doubles
  "Type-7 quantiles of a primitive `double-array`, the allocation-light form for
  the group-by hot path: compacts the finite values (drops NaN/±Inf) to the front
  of `buf` in place, sorts that prefix, and reads each probability in `ps` off the
  one sort — no boxing, no intermediate collection. **Mutates `buf`** (it must be
  scratch the caller owns). `ps` may be a single number or a vector; returns a
  scalar or vector to match. nil under the empty / `min-n` rules of quantile-type7."
  ([^doubles buf ps] (quantiles-of-doubles buf ps nil))
  ([^doubles buf ps min-n]
   (let [n (alength buf)
         w (loop [r 0 w 0]
             (if (< r n)
               (let [d (aget buf r)]
                 (if (or (Double/isNaN d) (Double/isInfinite d))
                   (recur (inc r) w)
                   (do (aset buf w d) (recur (inc r) (inc w)))))
               w))]
     (java.util.Arrays/sort buf 0 w)
     (if (sequential? ps)
       (mapv #(quantile-of-sorted buf w % min-n) ps)
       (quantile-of-sorted buf w ps min-n)))))
