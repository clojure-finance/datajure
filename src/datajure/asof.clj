(ns datajure.asof
  "Core sorted-merge algorithm for as-of join.

  Part 1 — asof-search / asof-indices:
    asof-search  — binary search for the last right index where value <= target.
    asof-indices — two-pointer merge over pre-sorted, pre-grouped vectors;
                   public utility, not used internally by asof-match.

  Part 2 — asof-match: full key-handling layer. Builds (or accepts a prebuilt)
  `:asof` index from datajure.index, then runs asof-search (binary search) per
  left row. Returns a lazy sequence of [left-row-idx right-row-idx-or-nil] pairs.

  Part 3 — build-result: assembles a tech.v3.dataset from the index pairs.
  Left columns always present in original order; right non-key columns
  appended (nil-filled for unmatched rows). Conflicting non-key column
  names suffixed :right.<n>.

  The right-side index construction lives in datajure.index (the `:asof` kind);
  this namespace owns the search (asof-search) and result assembly."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.dataset :as ds]
            [datajure.index :as idx]))

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

(defn- validate-asof-index!
  "Throw a structured ex-info if a user-supplied `:right-index` does not match
  this join's `right` dataset and `right-keys`. The index stores positional row
  ids into its source dataset, so it must be an :asof index built from exactly
  this `right` (identical?) on exactly these keys."
  [right-index right right-keys]
  (when (some? right-index)
    (when-not (idx/index? right-index)
      (throw (ex-info "asof :right-index must be a datajure index from (index-by … {:kind :asof})."
                      {:dt/error :asof-index-required :dt/value right-index})))
    (when-not (= :asof (idx/kind right-index))
      (throw (ex-info (str "asof :right-index must be of :kind :asof, got "
                           (idx/kind right-index) ".")
                      {:dt/error :asof-index-wrong-kind :dt/kind (idx/kind right-index)})))
    (when-not (identical? (idx/source-dataset right-index) right)
      (throw (ex-info (str "asof :right-index was built from a different dataset than "
                           "`right`; its row indices would be invalid.")
                      {:dt/error :asof-index-dataset-mismatch})))
    (when-not (= (idx/key-columns right-index) right-keys)
      (throw (ex-info (str "asof :right-index key columns " (idx/key-columns right-index)
                           " do not match right-keys " right-keys ".")
                      {:dt/error :asof-index-keys-mismatch
                       :dt/index-keys (idx/key-columns right-index)
                       :dt/right-keys right-keys})))))

(defn- resolve-asof-groups
  "Return the exact-key -> {:reader :orig :n} table to search against: from a
  validated prebuilt `right-index`, or freshly built from `right`/`right-keys`."
  [right right-keys right-index]
  (validate-asof-index! right-index right right-keys)
  (idx/asof-groups (or right-index (idx/asof-index right right-keys))))

(defn- within-tolerance?
  "Returns true if abs(left-val - right-val) <= tolerance.
  Always true when tolerance is nil. Requires numeric values."
  [left-val right-val tolerance]
  (or (nil? tolerance)
      (and (some? left-val) (some? right-val)
           (<= (Math/abs (- (double left-val) (double right-val)))
               (double tolerance)))))

(defn asof-row-indices
  "Core as-of matching. Returns {:li <long[]> :ri <long[]>}: for each left row,
  in left-row order, `li` is the left row index and `ri` the matched right row
  index (-1 = no match). Both arrays have length (row-count left). This is the
  primitive-array form asof-match and the join layer build on — it allocates no
  per-row pair objects.

  Arguments: left, right, left-keys, right-keys (last key col = asof, rest =
  exact), and `opts`:
    :direction   — :backward (default), :forward, or :nearest
    :tolerance   — numeric max abs distance; nil = unbounded. Requires a numeric
                   asof key. Matches exceeding it become no-match (-1).
    :right-index — (optional) a prebuilt :asof index over `right`/`right-keys`
                   (from datajure.index); reused instead of rebuilt. Validated
                   against `right` (identical?) and `right-keys`."
  [left right left-keys right-keys opts]
  (let [{:keys [direction tolerance right-index] :or {direction :backward}} opts
        left-rdrs (mapv #(dtype/->reader (ds/column left %)) left-keys)
        groups (resolve-asof-groups right right-keys right-index)
        nl (ds/row-count left)
        li-arr (long-array nl)
        ri-arr (long-array nl)]
    (dotimes [li nl]
      (aset li-arr li (long li))
      (let [exact (idx/row-exact-key left-rdrs li)
            av (idx/row-asof-val left-rdrs li)
            group (get groups exact)
            ri (if (nil? group)
                 -1
                 (let [{:keys [reader orig n]} group
                       local-ri (asof-search reader n av direction)]
                   (if (= local-ri -1)
                     -1
                     (if (within-tolerance? av (nth reader local-ri) tolerance)
                       (long (nth orig local-ri))
                       -1))))]
        (aset ri-arr li (long ri))))
    {:li li-arr :ri ri-arr}))

(defn asof-match
  "Produce index pairs for an as-of join: a lazy sequence of
  [left-row-idx right-row-idx-or-nil] in left-row order (unmatched left rows
  yield nil). Thin wrapper over `asof-row-indices`.

  Arities: 4-arg defaults; 5-arg opts map (:direction / :tolerance /
  :right-index — see asof-row-indices); 6-arg (… direction tolerance) kept for
  back-compatibility."
  ([left right left-keys right-keys]
   (asof-match left right left-keys right-keys {}))
  ([left right left-keys right-keys opts]
   (let [arrs (asof-row-indices left right left-keys right-keys opts)
         li (:li arrs)
         ri (:ri arrs)
         nl (alength ^longs li)]
     (map (fn [i]
            (let [r (aget ^longs ri i)]
              [(aget ^longs li i) (when (>= r 0) r)]))
          (range nl))))
  ([left right left-keys right-keys direction tolerance]
   (asof-match left right left-keys right-keys
               {:direction direction :tolerance tolerance})))

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

(defn build-result-arrays
  "Assemble the as-of join result from primitive li/ri arrays (ri = -1 for an
  unmatched left row). See `build-result` for the column layout. Each right
  column is gathered into a single object-array (unmatched positions stay nil),
  so peak working memory is one column-width rather than all right columns at
  once, and no per-row pair objects are allocated."
  [left right ^longs li-arr ^longs ri-arr right-keys]
  (let [data-cols (right-data-cols right right-keys)
        right-data (ds/select-columns right data-cols)
        left-col-set (set (ds/column-names left))
        right-named (rename-conflicts right-data left-col-set)
        final-cols (ds/column-names right-named)
        nl (alength li-arr)]
    (if (zero? nl)
      (merge left (nil-right-ds final-cols 0))
      (let [left-result (ds/select-rows left li-arr)
            right-result
            (reduce
             (fn [d col-name]
               (let [rdr (dtype/->reader (ds/column right-named col-name))
                     out (object-array nl)]
                 (dotimes [k nl]
                   (let [ri (aget ri-arr k)]
                     (when (>= ri 0)
                       (aset out k (nth rdr ri)))))
                 (ds/add-column d (ds/new-column col-name out))))
             (ds/->dataset {})
             final-cols)]
        (merge left-result right-result)))))

(defn build-result
  "Assemble the as-of join result dataset from index pairs (back-compat wrapper
  over `build-result-arrays`).

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
  (let [pv (vec pairs)
        nl (count pv)
        li-arr (long-array nl)
        ri-arr (long-array nl)]
    (dotimes [k nl]
      (let [p (nth pv k)
            ri (nth p 1)]
        (aset li-arr k (long (nth p 0)))
        (aset ri-arr k (long (if (nil? ri) -1 ri)))))
    (build-result-arrays left right li-arr ri-arr right-keys)))

(defn window-indices
  "For each left row, find all right row indices whose asof-key falls within
  [left-asof-key + lo-offset, left-asof-key + hi-offset] (both bounds inclusive).

  Arguments:
    left, right  — tech.v3.dataset
    left-keys    — column keywords (last = asof col, rest = exact-match cols)
    right-keys   — column keywords (same structure as left-keys)
    lo-offset    — lower bound offset added to left asof-key (numeric, raw units)
    hi-offset    — upper bound offset added to left asof-key (numeric, raw units)

  An opts-map arity `{:lo :hi :right-index}` is also accepted; `:right-index` is
  an optional prebuilt :asof index over `right`/`right-keys` (validated, reused
  instead of rebuilt).

  Returns a sequence of [left-row-idx [matched-right-original-row-indices]] pairs.
  Empty inner vector means no right rows fell in the window (left row still appears).
  Left rows with nil asof-key always yield an empty inner vector."
  ([left right left-keys right-keys lo-offset hi-offset]
   (window-indices left right left-keys right-keys {:lo lo-offset :hi hi-offset}))
  ([left right left-keys right-keys opts]
   (let [{:keys [lo hi right-index]} opts
         left-rdrs (mapv #(dtype/->reader (ds/column left %)) left-keys)
         groups (resolve-asof-groups right right-keys right-index)
         nl (ds/row-count left)]
     (for [li (range nl)]
       (let [exact (idx/row-exact-key left-rdrs li)
             av (idx/row-asof-val left-rdrs li)
             group (when (some? av) (get groups exact))]
         (if (nil? group)
           [li []]
           (let [{:keys [reader orig n]} group
                 lo-bound (+ (double av) (double lo))
                 hi-bound (+ (double av) (double hi))
                 lo-i (asof-search reader n lo-bound :forward)
                 hi-i (asof-search reader n hi-bound :backward)]
             (if (or (= lo-i -1) (= hi-i -1) (> lo-i hi-i))
               [li []]
               [li (subvec orig lo-i (inc hi-i))]))))))))

