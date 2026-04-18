(ns datajure.asof
  "Core sorted-merge algorithm for as-of join.

  Part 1 — asof-search / asof-indices:
    asof-search  — binary search for the last right index where value <= target.
    asof-indices — two-pointer merge over pre-sorted, pre-grouped vectors;
                   public utility, not used internally by asof-match.

  Part 2 — asof-match: full key-handling layer. Groups right rows by exact
  keys, sorts within each group, runs asof-search (binary search) per left
  row. Returns a lazy sequence of [left-row-idx right-row-idx-or-nil] pairs.

  Part 3 — build-result: assembles a tech.v3.dataset from the index pairs.
  Left columns always present in original order; right non-key columns
  appended (nil-filled for unmatched rows). Conflicting non-key column
  names suffixed :right.<n>."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset :as ds]))

;;; ---- Part 1 ----------------------------------------------------------------

(defn- asof-search-backward
  "Binary search: last index in `rdr` (sorted ascending, nils last) where value <= target.
  Returns -1 if no match."
  ^long [rdr ^long n target]
  (loop [lo 0 hi (dec n) result -1]
    (if (> lo hi)
      result
      (let [mid (quot (+ lo hi) 2)
            v (nth rdr mid)]
        (cond
          (nil? v) (recur lo (dec mid) result)
          (<= (compare v target) 0) (recur (inc mid) hi mid)
          :else (recur lo (dec mid) result))))))

(defn- asof-search-forward
  "Binary search: first index in `rdr` (sorted ascending, nils last) where value >= target.
  Returns -1 if no match."
  ^long [rdr ^long n target]
  (loop [lo 0 hi (dec n) result -1]
    (if (> lo hi)
      result
      (let [mid (quot (+ lo hi) 2)
            v (nth rdr mid)]
        (cond
          (nil? v) (recur lo (dec mid) result)
          (>= (compare v target) 0) (recur lo (dec mid) mid)
          :else (recur (inc mid) hi result))))))

(defn- asof-search-nearest
  "Returns the index of the closest value to `target` (either direction).
  On a tie (equidistant backward and forward match), returns the backward index."
  ^long [rdr ^long n target]
  (let [bi (asof-search-backward rdr n target)
        fi (asof-search-forward rdr n target)]
    (cond
      (= bi -1) fi
      (= fi -1) bi
      :else
      (let [bv (nth rdr bi)
            fv (nth rdr fi)
            bd-dist (Math/abs (- (double target) (double bv)))
            fd-dist (Math/abs (- (double fv) (double target)))]
        (if (<= bd-dist fd-dist) bi fi)))))

(defn asof-search
  "Binary search over sorted `right-vals` for the best match to `target`.
  Nils in right-vals are treated as greater than any real value (they sort last).
  Nil target always returns -1.

  direction (optional, default :backward):
    :backward — last index where right-val <= target  (default, SQL ASOF convention)
    :forward  — first index where right-val >= target
    :nearest  — closest index by absolute distance; ties prefer :backward

  Returns -1 if no qualifying match exists."
  (^long [right-vals ^long n target]
   (asof-search right-vals n target :backward))
  (^long [right-vals ^long n target direction]
   (if (or (zero? n) (nil? target))
     -1
     (let [rdr (dtype/->reader right-vals)]
       (case direction
         :backward (asof-search-backward rdr n target)
         :forward (asof-search-forward rdr n target)
         :nearest (asof-search-nearest rdr n target)
         (throw (ex-info (str "asof-search: unknown direction " direction
                              ". Must be :backward, :forward, or :nearest.")
                         {:dt/error :asof-unknown-direction :direction direction})))))))

(defn asof-indices
  "Two-pointer merge: for each element in `left-vals`, return a long-array
  of the matched right-row index within `right-vals` (both must be pre-sorted
  ascending). Returns -1 for left rows with no match. Nil values are skipped.

  Public utility — not used internally by asof-match, which uses asof-search
  (binary search per group) instead."
  [left-vals right-vals]
  (let [left-rdr (dtype/->reader left-vals)
        right-rdr (dtype/->reader right-vals)
        nl (dtype/ecount left-rdr)
        nr (dtype/ecount right-rdr)
        result (long-array nl -1)]
    (loop [li 0 ri 0]
      (when (and (< li nl) (< ri nr))
        (let [lv (nth left-rdr li)
              rv (nth right-rdr ri)]
          (cond
            (nil? lv) (recur (inc li) ri)
            (nil? rv) (recur li (inc ri))
            (pos? (compare rv lv)) (recur (inc li) ri)
            :else
            (do
              (aset result li (long ri))
              (if (< (inc ri) nr)
                (let [next-rv (nth right-rdr (inc ri))]
                  (if (and (not (nil? next-rv))
                           (<= (compare next-rv lv) 0))
                    (recur li (inc ri))
                    (recur (inc li) ri)))
                (loop [i (inc li)]
                  (when (< i nl)
                    (let [lv2 (nth left-rdr i)]
                      (when (and (not (nil? lv2))
                                 (>= (compare lv2 rv) 0))
                        (aset result i (long ri))))
                    (recur (inc i))))))))))
    result))

;;; ---- Part 2 ----------------------------------------------------------------

(defn- row-exact-key
  "Extract the exact-key tuple (all key columns except the last) from row `i`.
  Returns [] when there is only one key column (pure asof, no exact grouping)."
  [readers ^long i]
  (let [n (dec (count readers))]
    (loop [j 0 acc (transient [])]
      (if (= j n)
        (persistent! acc)
        (recur (inc j) (conj! acc (nth (nth readers j) i)))))))

(defn- row-asof-val
  "Extract the asof-key value (last key column) from row `i`."
  [readers ^long i]
  (nth (peek readers) i))

(defn- build-right-index
  "Return a map from exact-key-tuple -> sorted vector of [asof-val original-row-idx].
  Sorted ascending by asof-val; nils placed last.
  Right dataset need not be pre-sorted — sorting happens here."
  [dataset key-cols]
  (let [readers (mapv #(dtype/->reader (ds/column dataset %)) key-cols)
        n (ds/row-count dataset)
        grouped (reduce
                 (fn [acc i]
                   (update acc
                           (row-exact-key readers i)
                           (fnil conj [])
                           [(row-asof-val readers i) i]))
                 {}
                 (range n))
        asof-cmp (fn [a b]
                   (cond (nil? a) 1 (nil? b) -1 :else (compare a b)))]
    (reduce-kv
     (fn [m k pairs] (assoc m k (sort-by first asof-cmp pairs)))
     {}
     grouped)))

(defn- within-tolerance?
  "Returns true if abs(left-val - right-val) <= tolerance.
  Always true when tolerance is nil. Requires numeric values."
  [left-val right-val tolerance]
  (or (nil? tolerance)
      (and (some? left-val) (some? right-val)
           (<= (Math/abs (- (double left-val) (double right-val)))
               (double tolerance)))))

(defn asof-match
  "Produce index pairs for an as-of join.

  Arguments:
    left       — tech.v3.dataset
    right      — tech.v3.dataset
    left-keys  — vector of column keywords; last = asof col, rest = exact cols
    right-keys — vector of column keywords; same structure
    direction  — :backward (default), :forward, or :nearest
    tolerance  — numeric max abs distance; nil means unbounded. Requires a
                 numeric asof key. Matches whose distance exceeds tolerance
                 are treated as no-match (nil right index).

  Returns a lazy sequence of [left-row-idx right-row-idx-or-nil] pairs in
  left-row order. Unmatched left rows yield nil for right index."
  ([left right left-keys right-keys]
   (asof-match left right left-keys right-keys :backward nil))
  ([left right left-keys right-keys direction tolerance]
   (let [left-rdrs (mapv #(dtype/->reader (ds/column left %)) left-keys)
         right-index (build-right-index right right-keys)
         nl (ds/row-count left)]
     (for [li (range nl)]
       (let [exact (row-exact-key left-rdrs li)
             av (row-asof-val left-rdrs li)
             group (get right-index exact)]
         (if (nil? group)
           [li nil]
           (let [avals (mapv first group)
                 orig-idx (mapv second group)
                 local-ri (asof-search avals (count avals) av direction)]
             (if (= local-ri -1)
               [li nil]
               (let [matched-val (nth avals local-ri)]
                 (if (within-tolerance? av matched-val tolerance)
                   [li (nth orig-idx local-ri)]
                   [li nil]))))))))))

;;; ---- Part 3 ----------------------------------------------------------------

(defn- right-data-cols
  "Return the non-key column names from `right`."
  [right right-keys]
  (let [key-set (set right-keys)]
    (filterv #(not (contains? key-set %)) (ds/column-names right))))

(defn- rename-conflicts
  "Rename columns in `right-ds` that already appear in `left-col-set`
  by prefixing them with 'right.'. Returns the renamed dataset."
  [right-ds left-col-set]
  (reduce (fn [d col]
            (if (contains? left-col-set col)
              (ds/rename-columns d {col (keyword (str "right." (name col)))})
              d))
          right-ds
          (ds/column-names right-ds)))

(defn- nil-right-ds
  "Build an n-row dataset with nil values for each column in `col-names`."
  [col-names n]
  (reduce (fn [d col]
            (ds/add-column d (ds/new-column col (repeat n nil))))
          (ds/->dataset {})
          col-names))

(defn build-result
  "Assemble the as-of join result dataset from index pairs.

  Arguments:
    left       — tech.v3.dataset (all rows preserved, in original order)
    right      — tech.v3.dataset
    pairs      — seq of [left-row-idx right-row-idx-or-nil]
    right-keys — right key columns (dropped from the appended right columns)

  Returns a dataset with:
    - all left columns in original order
    - right non-key columns appended (nil-filled for unmatched rows)
    - conflicting non-key column names suffixed with :right.<n>"
  [left right pairs right-keys]
  (let [data-cols (right-data-cols right right-keys)
        right-data (ds/select-columns right data-cols)
        left-col-set (set (ds/column-names left))
        right-named (rename-conflicts right-data left-col-set)
        final-cols (ds/column-names right-named)
        pairs-vec (vec pairs)
        nl (count pairs-vec)]
    (if (zero? nl)
      (merge left (nil-right-ds final-cols 0))
      (let [right-rdrs (mapv #(dtype/->reader (ds/column right-named %)) final-cols)
            col-vecs (mapv (fn [rdr]
                             (mapv (fn [[_li ri]] (when ri (nth rdr ri)))
                                   pairs-vec))
                           right-rdrs)
            right-result (reduce (fn [d [col-name vals]]
                                   (ds/add-column d (ds/new-column col-name vals)))
                                 (ds/->dataset {})
                                 (map vector final-cols col-vecs))
            left-result (ds/select-rows left (mapv first pairs-vec))]
        (merge left-result right-result)))))

(defn window-indices
  "For each left row, find all right row indices whose asof-key falls within
  [left-asof-key + lo-offset, left-asof-key + hi-offset] (both bounds inclusive).

  Arguments:
    left, right  — tech.v3.dataset
    left-keys    — column keywords (last = asof col, rest = exact-match cols)
    right-keys   — column keywords (same structure as left-keys)
    lo-offset    — lower bound offset added to left asof-key (numeric, raw units)
    hi-offset    — upper bound offset added to left asof-key (numeric, raw units)

  Returns a sequence of [left-row-idx [matched-right-original-row-indices]] pairs.
  Empty inner vector means no right rows fell in the window (left row still appears).
  Left rows with nil asof-key always yield an empty inner vector."
  [left right left-keys right-keys lo-offset hi-offset]
  (let [left-rdrs (mapv #(dtype/->reader (ds/column left %)) left-keys)
        right-index (build-right-index right right-keys)
        nl (ds/row-count left)]
    (for [li (range nl)]
      (let [exact (row-exact-key left-rdrs li)
            av (row-asof-val left-rdrs li)
            group (when (some? av) (get right-index exact))]
        (if (nil? group)
          [li []]
          (let [avals (mapv first group)
                orig-idx (mapv second group)
                n (count avals)
                lo-bound (+ (double av) (double lo-offset))
                hi-bound (+ (double av) (double hi-offset))
                lo-i (asof-search avals n lo-bound :forward)
                hi-i (asof-search avals n hi-bound :backward)]
            (if (or (= lo-i -1) (= hi-i -1) (> lo-i hi-i))
              [li []]
              [li (subvec orig-idx lo-i (inc hi-i))])))))))

