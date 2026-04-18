(ns datajure.window
  "Window function implementations for datajure.
  Each function takes a column (dtype reader/vector) and returns a column
  of the same length. These are called per-partition by the expr compiler
  when processing :win AST nodes in window mode (:by + :set)."
  (:require [tech.v3.datatype :as dtype]))

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
  "Lag by offset positions. Returns nil for positions without enough history.
  [10 20 30 40], offset=1 -> [nil 10 20 30]"
  [col offset]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)
        off (long offset)]
    (dtype/make-reader :object n
                       (if (< idx off)
                         nil
                         (nth rdr (- idx off))))))

(defn win-lead
  "Lead by offset positions. Returns nil for positions without enough future.
  [10 20 30 40], offset=1 -> [20 30 40 nil]"
  [col offset]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)
        off (long offset)]
    (dtype/make-reader :object n
                       (let [target (+ idx off)]
                         (if (>= target n)
                           nil
                           (nth rdr target))))))

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

(defn- rolling-window-vals
  "Returns a vec of per-row window values. Each element is the result of
  applying f to the non-nil values in the window [i-width+1 .. i].
  Returns nil when the window contains no non-nil values."
  [col width f]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (loop [i 0 result (transient [])]
      (if (= i n)
        (persistent! result)
        (let [start (max 0 (- i (dec (long width))))
              window (loop [j start acc (transient [])]
                       (if (> j i)
                         (persistent! acc)
                         (let [v (nth rdr j)]
                           (recur (inc j)
                                  (if (some? v) (conj! acc (double v)) acc)))))]
          (recur (inc i)
                 (conj! result (when (seq window) (f window)))))))))

(defn win-mavg
  "Moving average over width rows (expanding at start). nil values skipped.
  Matches q's mavg convention.
  3 mavg [10 20 30 40 50] -> [10.0 15.0 20.0 30.0 40.0]"
  [col width]
  (dtype/->reader
   (rolling-window-vals col width
                        #(/ (reduce + %) (count %)))))

(defn win-msum
  "Moving sum over width rows (expanding at start). nil values skipped.
  3 msum [10 20 30 40 50] -> [10.0 30.0 60.0 90.0 120.0]"
  [col width]
  (dtype/->reader
   (rolling-window-vals col width #(reduce + %))))

(defn win-mdev
  "Moving standard deviation over width rows (expanding at start).
  Population std dev (ddof=0), matching q's mdev. nil values skipped.
  3 mdev [10 20 30 40 50] -> [0.0 5.0 8.165 8.165 8.165]"
  [col width]
  (dtype/->reader
   (rolling-window-vals col width
                        (fn [w]
                          (let [n (count w)
                                mu (/ (reduce + w) n)
                                variance (/ (reduce + (map #(let [d (- % mu)] (* d d)) w)) n)]
                            (Math/sqrt variance))))))

(defn win-mmin
  "Moving minimum over width rows (expanding at start). nil values skipped.
  3 mmin [30 10 50 20 40] -> [30.0 10.0 10.0 10.0 20.0]"
  [col width]
  (dtype/->reader
   (rolling-window-vals col width #(apply min %))))

(defn win-mmax
  "Moving maximum over width rows (expanding at start). nil values skipped.
  3 mmax [30 10 50 20 40] -> [30.0 30.0 50.0 50.0 50.0]"
  [col width]
  (dtype/->reader
   (rolling-window-vals col width #(apply max %))))

(defn win-ema
  "Exponential moving average. Parameter dispatch:
  - If period-or-alpha >= 1: treated as period, alpha = 2 / (1 + period)
  - If period-or-alpha < 1: treated directly as smoothing factor alpha
  Seeded at first non-nil value. nil values carry forward last EMA.
  Leading nils remain nil.
  ema 2 [10 20 30] -> [10.0 16.67 25.56]"
  [col period-or-alpha]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)
        a (double (if (>= (double period-or-alpha) 1.0)
                    (/ 2.0 (inc (double period-or-alpha)))
                    period-or-alpha))]
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

(defn win-fills
  "Forward-fill nil values with the last non-nil value.
  Leading nils (before the first non-nil) remain nil.
  Matches q's fills convention.
  [1 nil nil 4 nil] -> [1 1 1 4 4]"
  [col]
  (let [n (dtype/ecount col)
        rdr (dtype/->reader col)]
    (dtype/->reader
     (vec (loop [i 0 last-val nil acc []]
            (if (= i n)
              acc
              (let [v (nth rdr i)]
                (if (some? v)
                  (recur (inc i) v (conj acc v))
                  (recur (inc i) last-val (conj acc last-val))))))))))

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


