(ns datajure.window
  "Window function implementations for datajure.
  Each function takes a column (dtype reader/vector) and returns a column
  of the same length. These are called per-partition by the expr compiler
  when processing :win AST nodes in window mode (:by + :set)."
  (:require [tech.v3.datatype :as dtype]
            [datajure.math :as math]))

(defn win-rank
  "SQL RANK(): 1-based, min tie method, based on current row order.
  Ties (equal values) get the same rank, next rank skips.
  Pre-sorted [30 20 20 10] -> [1 2 2 4]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)
        result (long-array n)]
    (when (pos? n)
      (aset result 0 1)
      (loop [i 1]
        (when (< i n)
          (let [cur (nth rdr i)
                prev (nth rdr (dec i))]
            (aset result i
                  (if (= cur prev)
                    (aget result (dec i))
                    (inc (long i))))
            (recur (inc i))))))
    (dtype/->reader (vec (seq result)))))

(defn win-dense-rank
  "SQL DENSE_RANK(): 1-based, dense tie method, based on current row order.
  No gaps after ties. Pre-sorted [30 20 20 10] -> [1 2 2 3]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)
        result (long-array n)]
    (when (pos? n)
      (aset result 0 1)
      (loop [i 1 rank 1]
        (when (< i n)
          (let [cur (nth rdr i)
                prev (nth rdr (dec i))
                new-rank (if (= cur prev) rank (inc rank))]
            (aset result i new-rank)
            (recur (inc i) new-rank)))))
    (dtype/->reader (vec (seq result)))))

(defn win-row-number
  "SQL ROW_NUMBER(): 1-based sequential numbering by current row order."
  [col]
  (let [n (dtype/ecount col)]
    (dtype/->reader (vec (range 1 (inc n))))))

(defn win-lag
  "Lag by offset positions. Boundary positions (no history) are nil, or the `:fill`
  value from a trailing options map. Source nils that get lagged stay nil.
  [10 20 30 40], offset=1           -> [nil 10 20 30]
  [10 20 30 40], offset=1 {:fill 0} -> [0 10 20 30]"
  ([col offset] (win-lag col offset nil))
  ([col offset opts]
   (let [n (dtype/ecount col)
         rdr (dtype/->reader col)
         off (long offset)
         fill (:fill opts)]
     (dtype/make-reader :object n
                        (if (< idx off)
                          fill
                          (nth rdr (- idx off)))))))

(defn win-lead
  "Lead by offset positions. Boundary positions (no future) are nil, or the `:fill`
  value from a trailing options map. Source nils that get led stay nil.
  [10 20 30 40], offset=1           -> [20 30 40 nil]
  [10 20 30 40], offset=1 {:fill 0} -> [20 30 40 0]"
  ([col offset] (win-lead col offset nil))
  ([col offset opts]
   (let [n (dtype/ecount col)
         rdr (dtype/->reader col)
         off (long offset)
         fill (:fill opts)]
     (dtype/make-reader :object n
                        (let [target (+ idx off)]
                          (if (>= target n)
                            fill
                            (nth rdr target)))))))

(def ^:private tlag-units
  "Temporal units accepted by win-tlag's `:unit` option, mapped to ChronoUnit.
  `:quarter` has no ChronoUnit — it is handled as 3 months."
  {:day java.time.temporal.ChronoUnit/DAYS
   :week java.time.temporal.ChronoUnit/WEEKS
   :month java.time.temporal.ChronoUnit/MONTHS
   :year java.time.temporal.ChronoUnit/YEARS})

(defn- tlag-shifted-key
  "The date-key a row must match to be `shift` periods before `v`.
  Numbers shift by plain subtraction (normalised to double, matching the
  index keys); temporal values shift via java.time arithmetic per `unit`."
  [v shift unit]
  (if (number? v)
    (- (double v) (double shift))
    (let [n (long shift)]
      (if (= unit :quarter)
        (.minus ^java.time.temporal.Temporal v (* 3 n) java.time.temporal.ChronoUnit/MONTHS)
        (.minus ^java.time.temporal.Temporal v n ^java.time.temporal.ChronoUnit (tlag-units unit))))))

(defn win-tlag
  "Date-value-aware lag (mbmisc `lbd` / statar `tlag`): row i gets the value of
  `col` at the row whose `date-col` equals date[i] minus `shift` periods — nil
  when no such row exists. Unlike the positional `win/lag`, a gap in the panel
  yields nil instead of silently reaching back to the wrong period. A negative
  `shift` is a lead. `shift` defaults to 1.

  `date-col` values may be numbers (plain subtraction — years, xbar buckets,
  encoded periods) or java.time temporals, shifted per the `:unit` option
  (`:day` default, `:week`, `:month`, `:quarter`, `:year`). Exact-match caveat:
  calendar arithmetic must land on a date present in the data — for monthly
  panels keyed on month-END dates, tlag on a normalised month column (e.g. an
  `xbar` bucket) rather than the raw date.

  Dates must be unique within the partition (aggregate duplicates first, e.g.
  `:agg` by the keys); duplicates throw a structured :tlag-duplicate-dates
  error. nil dates yield nil and are never matched against.

  #dt/e: (win/tlag :x :year) / (win/tlag :ret :date 1 {:unit :month})"
  ([col date-col] (win-tlag col date-col 1 nil))
  ([col date-col shift] (win-tlag col date-col shift nil))
  ([col date-col shift opts]
   (let [rdr (dtype/->reader col)
         drdr (dtype/->reader date-col)
         n (dtype/ecount rdr)
         unit (:unit opts :day)]
     (when (not= n (dtype/ecount drdr))
       (throw (ex-info (str "win/tlag: value column and date column have different "
                            "lengths (" n " vs " (dtype/ecount drdr) ").")
                       {:dt/error :unequal-column-lengths
                        :dt/lengths [n (dtype/ecount drdr)]})))
     (when-not (number? shift)
       (throw (ex-info (str "win/tlag: shift must be a number; got " (pr-str shift) ".")
                       {:dt/error :tlag-invalid-shift :dt/shift shift})))
     (when-not (contains? tlag-units (if (= unit :quarter) :month unit))
       (throw (ex-info (str "win/tlag: unknown :unit " (pr-str unit)
                            ". Supported: :day :week :month :quarter :year.")
                       {:dt/error :tlag-unknown-unit :dt/unit unit})))
     (let [key-of (fn [v] (if (number? v) (double v) v))
           idx-map (loop [i 0 m (transient {})]
                     (if (= i n)
                       (persistent! m)
                       (let [dv (nth drdr i)]
                         (if (nil? dv)
                           (recur (inc i) m)
                           (let [k (key-of dv)]
                             (if (some? (get m k))
                               (throw (ex-info
                                       (str "win/tlag: duplicate date value " (pr-str dv)
                                            " in partition — dates must be unique per "
                                            "partition. Aggregate duplicates first "
                                            "(e.g. :agg by the id/date keys).")
                                       {:dt/error :tlag-duplicate-dates
                                        :dt/date-value dv}))
                               (recur (inc i) (assoc! m k i))))))))]
       (dtype/make-reader :object n
                          (let [dv (nth drdr idx)]
                            (when (some? dv)
                              (when-let [j (get idx-map (tlag-shifted-key dv shift unit))]
                                (nth rdr j)))))))))

(defn win-cumsum
  "Cumulative sum. nil values treated as 0.
  [10 20 30] -> [10 30 60]
  [nil 20 30] -> [0.0 20.0 50.0]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (dtype/->reader
     (loop [i 0 acc 0.0 result (transient [])]
       (if (= i n)
         (persistent! result)
         (let [v (nth rdr i)
               new-acc (if (some? v) (+ acc (double v)) acc)]
           (recur (inc i) new-acc (conj! result new-acc))))))))

(defn win-cummin
  "Cumulative minimum. nil values skipped. Leading nils remain nil.
  [nil nil 5.0 3.0] -> [nil nil 5.0 3.0]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (dtype/->reader
     (loop [i 0 acc nil result (transient [])]
       (if (= i n)
         (persistent! result)
         (let [v (nth rdr i)]
           (cond
             (nil? v) (recur (inc i) acc (conj! result nil))
             (nil? acc) (recur (inc i) (double v) (conj! result (double v)))
             :else (let [new-acc (min acc (double v))]
                     (recur (inc i) new-acc (conj! result new-acc))))))))))

(defn win-cummax
  "Cumulative maximum. nil values skipped. Leading nils remain nil.
  [nil nil 5.0 8.0] -> [nil nil 5.0 8.0]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (dtype/->reader
     (loop [i 0 acc nil result (transient [])]
       (if (= i n)
         (persistent! result)
         (let [v (nth rdr i)]
           (cond
             (nil? v) (recur (inc i) acc (conj! result nil))
             (nil? acc) (recur (inc i) (double v) (conj! result (double v)))
             :else (let [new-acc (max acc (double v))]
                     (recur (inc i) new-acc (conj! result new-acc))))))))))

(defn win-cummean
  "Cumulative mean. nil values skipped.
  [10 20 30] -> [10.0 15.0 20.0]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (dtype/->reader
     (vec (loop [i 0 cnt 0 sm 0.0 acc []]
            (if (= i n)
              acc
              (let [v (nth rdr i)]
                (if (some? v)
                  (let [c (inc cnt)
                        s (+ sm (double v))]
                    (recur (inc i) c s (conj acc (/ s c))))
                  (recur (inc i) cnt sm
                         (conj acc (if (pos? cnt) (/ sm cnt) nil)))))))))))

(defn win-rleid
  "Run-length encoding group ID. Increments when value changes.
  [A A A B B A A] -> [1 1 1 2 2 3 3]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (if (zero? n)
      (dtype/->reader [])
      (dtype/->reader
       (vec (first
             (reduce (fn [[acc prev gid] i]
                       (let [v (nth rdr i)]
                         (if (= v prev)
                           [(conj acc gid) v gid]
                           [(conj acc (inc gid)) v (inc gid)])))
                     [[(long 1)] (nth rdr 0) (long 1)]
                     (range 1 n))))))))

(defn win-delta
  "Difference from previous element: x[i] - x[i-1].
  Returns nil for the first element (no predecessor).
  [10 20 30] -> [nil 10 10]"
  [col]
  (let [n (dtype/ecount col)
        lagged (win-lag col 1)
        lag-rdr (dtype/->reader lagged)
        rdr (dtype/->reader col)]
    (dtype/make-reader :object n
                       (let [cur (nth rdr idx)
                             prev (nth lag-rdr idx)]
                         (if (or (nil? cur) (nil? prev))
                           nil
                           (- (double cur) (double prev)))))))

(defn win-grr
  "Inverse-hyperbolic-sine growth (mbmisc `grr` with IHS=TRUE):
  asinh(x[i]) - asinh(x[i-1]) over the current partition/order. nil for the
  first element (no predecessor). A run of zeros has zero growth: when both
  x[i] and x[i-1] are 0, the result is 0.0 (not asinh(0)-asinh(0)). A nil or
  non-finite operand yields nil.

  [1 2 3]   -> [nil (- (asinh 2) (asinh 1)) (- (asinh 3) (asinh 2))]
  [0 0 5]   -> [nil 0.0 (- (asinh 5) (asinh 0))]"
  [col]
  (let [n (dtype/ecount col)
        lagged (win-lag col 1)
        lag-rdr (dtype/->reader lagged)
        rdr (dtype/->reader col)]
    (dtype/make-reader :object n
                       (let [cur (nth rdr idx)
                             prev (nth lag-rdr idx)]
                         (when (and (math/finite-double? cur) (math/finite-double? prev))
                           (if (and (zero? (double cur)) (zero? (double prev)))
                             0.0
                             (- (math/asinh cur) (math/asinh prev))))))))

(defn win-ratio
  "Ratio to previous element: x[i] / x[i-1].
  Returns nil for the first element (no predecessor) and nil when the
  previous element is zero (avoids Infinity propagation in financial data,
  matching the div0 philosophy). The simple-return idiom
  `(- (win/ratio :price) 1)` then yields nil for the observation after a
  zero-price row, signalling 'exclude this observation' rather than
  polluting downstream calculations with Infinity.

  [10 20 30]       -> [nil 2.0 1.5]
  [100 0 50 100]   -> [nil 0.0 nil 2.0]"
  [col]
  (let [n (dtype/ecount col)
        lagged (win-lag col 1)
        lag-rdr (dtype/->reader lagged)
        rdr (dtype/->reader col)]
    (dtype/make-reader :object n
                       (let [cur (nth rdr idx)
                             prev (nth lag-rdr idx)]
                         (if (or (nil? cur) (nil? prev) (zero? (double prev)))
                           nil
                           (/ (double cur) (double prev)))))))

(defn win-differ
  "Boolean: true where value differs from predecessor.
  First element always returns true (q convention — no predecessor to match).
  [A A B B A] -> [true false true false true]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (dtype/make-reader :boolean n
                       (if (zero? idx)
                         true
                         (not= (nth rdr idx) (nth rdr (dec idx)))))))

(defn- win-opts
  "Normalise a rolling op's optional trailing arg to an options map. A map passes
  through; a bare number is read as `:ddof` (win-mdev positional back-compat);
  nil/absent → {}. Recognised keys:
    :min-periods — minimum window SPAN (rows) before a value is emitted; default 1
                   (expanding, q convention). Set to `width` for a non-expanding /
                   R `zoo::rollapplyr` window (leading width-1 rows → nil).
    :ddof        — win-mdev only (delta degrees of freedom)."
  [opt]
  (cond (map? opt) opt
        (number? opt) {:ddof opt}
        :else {}))

(defn- rolling-prim
  "Primitive trailing-window engine for the win/m* ops. Realises the column once into
  a `double[]` + a `present` mask (one O(n) pass, the only boxing), then for each row
  applies `reducer` to the window [max(0,i-width+1) .. i] with no per-window allocation
  or boxing. Emits the reducer's result (a Double, or nil) when the window spans
  `>= min-periods` rows; otherwise nil. `reducer` is
  (fn [^doubles darr ^booleans present start i] -> Double|nil), reading only present
  slots. Present-but-non-finite values are kept (so they poison sums, as before);
  nil/missing slots are skipped."
  [col width min-periods reducer]
  (let [rdr (dtype/->reader col)
        n (long (dtype/ecount col))
        present (boolean-array n)
        darr (double-array n)
        out (object-array n)
        w (long width)
        mp (long min-periods)]
    (dotimes [j n]
      (let [v (nth rdr j)]
        (when (some? v) (aset present j true) (aset darr j (double v)))))
    (dotimes [i n]
      (let [start (max 0 (- (inc i) w))]
        (when (>= (- (inc i) start) mp)
          (aset out i (reducer darr present start i)))))
    (dtype/->reader out)))

(defn- r-msum [darr present start i]
  (let [^doubles darr darr ^booleans present present e (long i)]
    (loop [j (long start) sum 0.0 cnt 0]
      (if (> j e)
        (when (pos? cnt) sum)
        (if (aget present j)
          (recur (inc j) (+ sum (aget darr j)) (inc cnt))
          (recur (inc j) sum cnt))))))

(defn- r-mavg [darr present start i]
  (let [^doubles darr darr ^booleans present present e (long i)]
    (loop [j (long start) sum 0.0 cnt 0]
      (if (> j e)
        (when (pos? cnt) (/ sum (double cnt)))
        (if (aget present j)
          (recur (inc j) (+ sum (aget darr j)) (inc cnt))
          (recur (inc j) sum cnt))))))

(defn- r-mmin [darr present start i]
  (let [^doubles darr darr ^booleans present present e (long i)]
    (loop [j (long start) m 0.0 seen false]
      (if (> j e)
        (when seen m)
        (if (aget present j)
          (let [v (aget darr j)] (recur (inc j) (if seen (min m v) v) true))
          (recur (inc j) m seen))))))

(defn- r-mmax [darr present start i]
  (let [^doubles darr darr ^booleans present present e (long i)]
    (loop [j (long start) m 0.0 seen false]
      (if (> j e)
        (when seen m)
        (if (aget present j)
          (let [v (aget darr j)] (recur (inc j) (if seen (max m v) v) true))
          (recur (inc j) m seen))))))

(defn- r-mdev
  "Returns a reducer for sample/population sd with the given ddof (divisor n-ddof)."
  [ddof]
  (let [dd (long ddof)]
    (fn [darr present start i]
      (let [^doubles darr darr ^booleans present present s (long start) e (long i)]
        (loop [j s sum 0.0 cnt 0]
          (if (> j e)
            (let [denom (- cnt dd)]
              (when (pos? denom)
                (let [mu (/ sum (double cnt))]
                  (loop [k s ss 0.0]
                    (if (> k e)
                      (Math/sqrt (/ ss (double denom)))
                      (if (aget present k)
                        (let [d (- (aget darr k) mu)] (recur (inc k) (+ ss (* d d))))
                        (recur (inc k) ss)))))))
            (if (aget present j)
              (recur (inc j) (+ sum (aget darr j)) (inc cnt))
              (recur (inc j) sum cnt))))))))

(defn- r-mdowndev [darr present start i]
  (let [^doubles darr darr ^booleans present present e (long i)]
    (loop [j (long start) ss 0.0 m 0]
      (if (> j e)
        (when (pos? m) (Math/sqrt (/ ss (double m))))
        (if (aget present j)
          (let [v (aget darr j)]
            (if (and (not (Double/isNaN v)) (not (Double/isInfinite v)))   ; finite only
              (recur (inc j) (+ ss (if (neg? v) (* v v) 0.0)) (inc m))
              (recur (inc j) ss m)))
          (recur (inc j) ss m))))))

(defn win-mavg
  "Moving average over width rows (expanding at start). nil values skipped.
  Matches q's mavg convention. Optional opts map: `{:min-periods n}` requires an
  n-row window before emitting (e.g. `width` for a non-expanding R-style window).
  3 mavg [10 20 30 40 50] -> [10.0 15.0 20.0 30.0 40.0]"
  ([col width] (win-mavg col width nil))
  ([col width opts]
   (rolling-prim col width (:min-periods (win-opts opts) 1) r-mavg)))

(defn win-msum
  "Moving sum over width rows (expanding at start). nil values skipped. Optional
  opts map: `{:min-periods n}` (default 1; `width` for a non-expanding window).
  3 msum [10 20 30 40 50] -> [10.0 30.0 60.0 90.0 120.0]"
  ([col width] (win-msum col width nil))
  ([col width opts]
   (rolling-prim col width (:min-periods (win-opts opts) 1) r-msum)))

(defn win-mdev
  "Moving standard deviation over `width` rows (expanding at start). `ddof` (delta
  degrees of freedom) sets the divisor `n - ddof`:
    ddof=1 (default) — sample sd, matching R's `sd` and datajure's `sd` aggregator;
    ddof=0           — population sd, matching q's `mdev`.
  nil values skipped. A window with `n <= ddof` finite values yields nil (sample sd
  of a single value is undefined, like R's `sd`).
  The optional 3rd arg is either a bare `ddof` (back-compat) or an opts map
  `{:ddof d :min-periods n}` (`:min-periods` default 1; `width` for a non-expanding
  R `sd`/`rollapplyr` window).
  3 mdev [10 20 30 40 50]   -> [nil 7.071 10.0 10.0 10.0]   (ddof=1, default)
  3 mdev [10 20 30 40 50] 0 -> [0.0 5.0 8.165 8.165 8.165]  (ddof=0, q's mdev)"
  ([col width] (win-mdev col width nil))
  ([col width opt]
   (let [o (win-opts opt)]
     (rolling-prim col width (:min-periods o 1) (r-mdev (:ddof o 1))))))

(defn win-mdowndev
  "Moving downside deviation over `width` rows (expanding at start), MAR=0:
  sqrt(mean(min(r,0)^2)) over the finite values in each trailing window, with the
  count of finite values as the denominator (PerformanceAnalytics
  DownsideDeviation, method='full', na.rm). nil/NaN/±Inf are skipped; a window
  with no finite values (empty / all-missing) yields nil — undefined, no data to
  deviate from — a deliberate divergence from R's DownsideDeviation (which returns
  0), matching datajure's nil-for-undefined philosophy. A window with finite values
  but no downside returns 0.0. Optional opts map: `{:min-periods n}` (default 1;
  `width` for a non-expanding R `rollapplyr` window — the `roll-dd` convention).
  3 mdowndev [1.0 2.0 -2.0] -> [0.0 0.0 ~1.1547]"
  ([col width] (win-mdowndev col width nil))
  ([col width opts]
   (rolling-prim col width (:min-periods (win-opts opts) 1) r-mdowndev)))

(defn win-mmin
  "Moving minimum over width rows (expanding at start). nil values skipped. Optional
  opts map: `{:min-periods n}` (default 1; `width` for a non-expanding window).
  3 mmin [30 10 50 20 40] -> [30.0 10.0 10.0 10.0 20.0]"
  ([col width] (win-mmin col width nil))
  ([col width opts]
   (rolling-prim col width (:min-periods (win-opts opts) 1) r-mmin)))

(defn win-mmax
  "Moving maximum over width rows (expanding at start). nil values skipped. Optional
  opts map: `{:min-periods n}` (default 1; `width` for a non-expanding window).
  3 mmax [30 10 50 20 40] -> [30.0 30.0 50.0 50.0 50.0]"
  ([col width] (win-mmax col width nil))
  ([col width opts]
   (rolling-prim col width (:min-periods (win-opts opts) 1) r-mmax)))

(defn win-ema
  "Exponential moving average. The smoothing parameter accepts three forms:
  - a number >= 1   → treated as period, alpha = 2 / (1 + period)
  - a number < 1    → treated directly as smoothing factor alpha
  - an options map   → `{:alpha a}` sets alpha directly, or `{:period p}` sets it via
                       the period formula — self-documenting alternatives to the
                       implicit numeric dispatch.
  Seeded at first non-nil value. nil values carry forward last EMA.
  Leading nils remain nil.
  ema 2 [10 20 30]            -> [10.0 16.67 25.56]
  ema {:alpha 0.18} [10 …]    -> same as ema 0.18 …"
  [col period-or-alpha]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)
        a (double (cond
                    (map? period-or-alpha)
                    (let [{:keys [alpha period]} period-or-alpha]
                      (cond (some? alpha) (double alpha)
                            (some? period) (/ 2.0 (inc (double period)))
                            :else (throw (ex-info "win/ema options map requires :alpha or :period"
                                                  {:dt/error :ema-opts :opts period-or-alpha}))))
                    (>= (double period-or-alpha) 1.0) (/ 2.0 (inc (double period-or-alpha)))
                    :else (double period-or-alpha)))]
    (dtype/->reader
     (vec (loop [i 0 prev nil acc []]
            (if (= i n)
              acc
              (let [v (nth rdr i)]
                (cond
                  (nil? v) (recur (inc i) prev (conj acc prev))
                  (nil? prev) (recur (inc i) (double v) (conj acc (double v)))
                  :else (let [e (+ (* a (double v)) (* (- 1.0 a) prev))]
                          (recur (inc i) e (conj acc e)))))))))))

(defn- fill-limit
  "Normalise a fill op's optional trailing arg to a positive long carry limit.
  A bare number or {:limit n} → n; nil/absent → unlimited. A non-positive
  limit, a non-number, or an options map without :limit is an error — a map
  like {:lmit 3} silently meaning \"unlimited\" would be a footgun."
  [fn-name opt]
  (let [limit (cond
                (nil? opt) Long/MAX_VALUE
                (number? opt) (long opt)
                (and (map? opt) (contains? opt :limit)) (long (:limit opt))
                :else (throw (ex-info (str fn-name ": expected a bare number or an options map "
                                           "with :limit; got " (pr-str opt) ".")
                                      {:dt/error :fills-invalid-limit :dt/opt opt})))]
    (if (pos? limit)
      limit
      (throw (ex-info (str fn-name ": :limit must be positive; got " limit ".")
                      {:dt/error :fills-invalid-limit :dt/limit limit})))))

(defn win-fills
  "Forward-fill nil values with the last non-nil value.
  Leading nils (before the first non-nil) remain nil.
  Matches q's fills convention.

  An optional limit — a bare number or {:limit n} — carries the value at most
  n positions into each nil run, leaving the rest of the run nil (partial
  fill: pandas `ffill(limit=n)` / mbmisc `h.locf`; deliberately NOT zoo's
  all-or-nothing `maxgap`). Default: unlimited.

  [1 nil nil 4 nil]           -> [1 1 1 4 4]
  [1 nil nil 4 nil] {:limit 1} -> [1 1 nil 4 4]"
  ([col] (win-fills col nil))
  ([col opt]
   (let [limit (fill-limit "win/fills" opt)
         n (dtype/ecount col)
         rdr (dtype/->reader col)]
     (dtype/->reader
      (vec (loop [i 0 last-val nil carried 0 acc []]
             (if (= i n)
               acc
               (let [v (nth rdr i)]
                 (cond
                   (some? v) (recur (inc i) v 0 (conj acc v))
                   (and (some? last-val) (< carried limit))
                   (recur (inc i) last-val (inc carried) (conj acc last-val))
                   :else (recur (inc i) last-val (inc carried) (conj acc nil)))))))))))

(defn win-bfill
  "Backward-fill nil values with the next non-nil value (NOCB — the mirror of
  [[win-fills]]). Trailing nils (after the last non-nil) remain nil. Takes the
  same optional limit (bare number or {:limit n}) with the same partial-fill
  semantics. NOCB-then-LOCF combos compose by nesting:
  #dt/e (win/fills (win/bfill :x)).

  [nil 1 nil nil 4] -> [1 1 4 4 4]"
  ([col] (win-bfill col nil))
  ([col opt]
   (let [limit (fill-limit "win/bfill" opt)
         n (dtype/ecount col)
         rdr (dtype/->reader col)]
     (dtype/->reader
      (vec (loop [i (dec n) next-val nil carried 0 acc ()]
             (if (neg? i)
               acc
               (let [v (nth rdr i)]
                 (cond
                   (some? v) (recur (dec i) v 0 (conj acc v))
                   (and (some? next-val) (< carried limit))
                   (recur (dec i) next-val (inc carried) (conj acc next-val))
                   :else (recur (dec i) next-val (inc carried) (conj acc nil)))))))))))

(def scan-op-table
  "Maps op keywords to binary functions for use in win-scan."
  {:+ +, :* *, :max max, :min min})

(defn win-scan
  "Generalized cumulative scan: applies a binary op left-to-right across col
  (like Clojure's reductions). Nil values are skipped (last good value carried).
  Leading nils remain nil until first non-nil value is found.

  op-kw must be one of :+ :* :max :min.
  Killer use case: cumulative compounding wealth index via :*

  Examples:
    win-scan :+  [1 2 3 4]  -> [1 3 6 10]
    win-scan :*  [1.1 1.2 1.3] -> [1.1 1.32 1.716]
    win-scan :max [30 10 50 20] -> [30 30 50 50]"
  [op-kw col]
  (let [f (or (scan-op-table op-kw)
              (throw (ex-info (str "win/scan: unsupported operator " op-kw
                                   ". Supported: :+ :* :max :min")
                              {:dt/error :win-scan-unknown-op :op op-kw})))
        n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (dtype/->reader
     (vec (loop [i 0 acc nil result []]
            (if (= i n)
              result
              (let [v (nth rdr i)]
                (cond
                  (nil? v) (recur (inc i) acc (conj result acc))
                  (nil? acc) (recur (inc i) v (conj result v))
                  :else (let [nxt (f acc v)]
                          (recur (inc i) nxt (conj result nxt)))))))))))

(def each-prior-op-table
  "Binary operators supported by win/each-prior."
  {:+ + :- - :* * :div / :max max :min min
   :> > :< < :>= >= :<= <= := =})

(defn win-each-prior
  "Apply a binary operator to (f x[i] x[i-1]) for each element.
  Returns nil for the first element (no predecessor).
  Nil propagates: if either x[i] or x[i-1] is nil, result is nil.

  op-kw must be one of: :+ :- :* :div :max :min :> :< :>= :<= :=

  Generalizes win/delta (op=:-) and win/ratio (op=:div), but without
  the double-casting of win/delta or the zero-guard of win/ratio.
  Use win/delta or win/ratio directly when those semantics are needed.

  Examples:
    (win-each-prior :- [10.0 20.0 30.0]) -> [nil 10.0 10.0]
    (win-each-prior :div [10.0 20.0 30.0]) -> [nil 2.0 1.5]
    (win-each-prior :max [30.0 10.0 50.0]) -> [nil 30.0 50.0]"
  [op-kw col]
  (let [f (or (each-prior-op-table op-kw)
              (throw (ex-info (str "win/each-prior: unsupported operator " op-kw
                                   ". Supported: :+ :- :* :div :max :min :> :< :>= :<= :=")
                              {:dt/error :each-prior-unknown-op :op op-kw})))
        rdr (dtype/->reader col)
        n (dtype/ecount rdr)]
    (dtype/make-reader :object n
                       (if (zero? idx)
                         nil
                         (let [cur (nth rdr idx)
                               prev (nth rdr (dec idx))]
                           (when (and (some? cur) (some? prev))
                             (f cur prev)))))))


