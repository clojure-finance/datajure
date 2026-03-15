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

(defn asof-search
  "Binary search: find the last index in sorted `right-vals` where value <= `target`.
  Nils in right-vals are treated as greater than any real value (they sort last),
  so a nil at mid causes the search to go left, not right.
  Returns -1 if no such element exists. Nil target returns -1."
  ^long [right-vals ^long n target]
  (if (or (zero? n) (nil? target))
    -1
    (let [rdr (dtype/->reader right-vals)]
      (loop [lo 0 hi (dec n) result -1]
        (if (> lo hi)
          result
          (let [mid (quot (+ lo hi) 2)
                v (nth rdr mid)]
            (cond
              ;; nil sorts last => treat as "too big", search left half
              (nil? v) (recur lo (dec mid) result)
              (<= (compare v target) 0) (recur (inc mid) hi mid)
              :else (recur lo (dec mid) result))))))))

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

(defn asof-match
  "Produce index pairs for an as-of join.

  Arguments:
    left       — tech.v3.dataset
    right      — tech.v3.dataset
    left-keys  — vector of column keywords; last = asof col, rest = exact cols
    right-keys — vector of column keywords; same structure

  Returns a lazy sequence of [left-row-idx right-row-idx-or-nil] pairs in
  left-row order. Unmatched left rows (no exact-key group, or asof-key
  earlier than all right asof-keys for that group) yield nil for right index."
  [left right left-keys right-keys]
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
                local-ri (asof-search avals (count avals) av)]
            (if (= local-ri -1)
              [li nil]
              [li (nth orig-idx local-ri)])))))))

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
