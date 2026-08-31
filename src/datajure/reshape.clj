(ns datajure.reshape
  "Reshape functions for datajure: wide->long (melt), long->wide (cast), and
  panel gap-filling (tsfill)."
  (:refer-clojure :exclude [cast])
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [datajure.math :as math]
            [datajure.window :as win]))

(defn melt
  "Reshape a dataset from wide to long format.

  Arguments:
    dataset  - a tech.v3.dataset
    opts     - map with keys:
      :id           - (required) vector of column keywords to keep as identifiers
      :measure      - vector of column keywords to stack. Defaults to all non-id columns.
      :variable-col - keyword for the new variable column. Defaults to :variable.
      :value-col    - keyword for the new value column. Defaults to :value.

  Returns a dataset with one row per (id, measure) combination.
  If no measure columns exist (all columns are id columns), returns an empty dataset
  with the id columns plus the variable and value columns.

  Examples:
    ;; Basic melt
    (melt ds {:id [:species :year] :measure [:mass :flipper :bill]})

    ;; Infer measure cols (all non-id)
    (melt ds {:id [:species :year]})

    ;; Custom output column names
    (melt ds {:id [:species :year] :measure [:mass :flipper]
              :variable-col :metric :value-col :val})"
  [dataset {:keys [id measure variable-col value-col]
            :or {variable-col :variable value-col :value}}]
  (let [measure-cols (or measure (remove (set id) (ds/column-names dataset)))]
    (if (empty? measure-cols)
      (-> (ds/select-columns dataset (vec id))
          (ds/select-rows [])
          (ds/add-column (ds/new-column variable-col []))
          (ds/add-column (ds/new-column value-col [])))
      (apply ds/concat
             (map (fn [mcol]
                    (-> dataset
                        (ds/select-columns (conj (vec id) mcol))
                        (ds/rename-columns {mcol value-col})
                        (ds/add-column (ds/new-column variable-col
                                                      (repeat (ds/row-count dataset) (name mcol))))))
                  measure-cols)))))

(defn- cast-col-name
  "Convert a :from column value to a keyword for use as a dataset column name.
  Strings and other non-keywords are converted via (keyword (str v))."
  [v]
  (if (keyword? v) v (keyword (str v))))

(defn cast
  "Reshape a dataset from long to wide format. Complement to melt.

  For each unique combination of :id column values, pivots the :from column's
  distinct values into new columns filled from the :value column. New column
  names are derived from the :from values (keywords passed through; strings
  and other types converted via keyword). They appear in order of first
  occurrence in the :from column.

  Arguments:
    dataset - a tech.v3.dataset
    opts    - map with keys:
      :id    - (required) column keyword or vector of column keywords to use
               as row identifiers. A single keyword is normalised to a
               one-element vector, matching melt.
      :from  - (required) column keyword whose unique values become new column names
      :value - (required) column keyword whose values fill the new columns
      :agg   - aggregation fn applied to a vector of values when multiple rows
               share the same (id, from) combination. Default: use the first value.
      :fill  - value for cells with no matching (id, from) row. Default: nil.

  Examples:
    ;; Reverse a melt
    (-> ds
        (melt {:id [:species :year] :measure [:mass :flipper]})
        (cast {:id [:species :year] :from :variable :value :value}))

    ;; Single-keyword :id also works
    (cast ds {:id :year :from :metric :value :val})

    ;; With aggregation for duplicate cells
    (cast ds {:id [:date :sym] :from :metric :value :val :agg dfn/mean})"
  [dataset {:keys [id from value agg fill]
            :or {fill nil}}]
  (when-not id (throw (ex-info "cast requires :id" {:dt/error :cast-missing-id})))
  (when-not from (throw (ex-info "cast requires :from" {:dt/error :cast-missing-from})))
  (when-not value (throw (ex-info "cast requires :value" {:dt/error :cast-missing-value})))
  (let [id (if (keyword? id) [id] (vec id))
        id-rdrs (mapv #(dtype/->reader (ds/column dataset %)) id)
        from-rdr (dtype/->reader (ds/column dataset from))
        val-rdr (dtype/->reader (ds/column dataset value))
        n (ds/row-count dataset)
        new-cols (vec (distinct (map cast-col-name from-rdr)))
        idx (reduce
             (fn [acc i]
               (let [id-key (mapv #(nth % i) id-rdrs)
                     col (cast-col-name (nth from-rdr i))
                     v (nth val-rdr i)]
                 (update-in acc [id-key col] (fnil conj []) v)))
             {}
             (range n))
        id-order (vec (distinct (map (fn [i] (mapv #(nth % i) id-rdrs))
                                     (range n))))
        rows (mapv (fn [id-key]
                     (let [cell-map (get idx id-key {})]
                       (merge
                        (zipmap id id-key)
                        (into {}
                              (map (fn [col]
                                     [col (let [vals (get cell-map col [])]
                                            (cond (empty? vals) fill
                                                  agg (agg vals)
                                                  :else (first vals)))])
                                   new-cols)))))
                   id-order)]
    (reduce (fn [d col-kw]
              (ds/add-column d (ds/new-column col-kw (mapv #(get % col-kw) rows))))
            (ds/->dataset {})
            (into id new-cols))))

;; ---------------------------------------------------------------------------
;; tsfill — panel gap-filling
;; ---------------------------------------------------------------------------

(def ^:private tsfill-chrono-units
  "Temporal units accepted by tsfill's `:every` option, mapped to [n ChronoUnit]
  (`:quarter` has no ChronoUnit — it is 3 months)."
  {:day [1 java.time.temporal.ChronoUnit/DAYS]
   :week [1 java.time.temporal.ChronoUnit/WEEKS]
   :month [1 java.time.temporal.ChronoUnit/MONTHS]
   :quarter [3 java.time.temporal.ChronoUnit/MONTHS]
   :year [1 java.time.temporal.ChronoUnit/YEARS]})

(defn- tsfill-grid
  "The regular date grid for one group: lo, lo+every, … up to hi (inclusive).
  Numeric dates step by a number (grid points computed as lo + k*step, no
  accumulation drift; longs stay longs for a whole step); temporal dates step
  by a unit keyword via ChronoUnit arithmetic anchored at lo."
  [lo hi every]
  (if (number? lo)
    (let [every (or every 1)
          _ (when-not (and (number? every) (pos? (double every)))
              (throw (ex-info (str "tsfill: :every for a numeric date column must be a "
                                   "positive number; got " (pr-str every) ".")
                              {:dt/error :tsfill-invalid-every :dt/every every})))
          step (double every)
          long-grid? (and (integer? lo) (== step (Math/rint step)))]
      (loop [k 0 acc []]
        (let [p (+ (double lo) (* k step))]
          (if (> p (+ (double hi) (* 1e-9 step)))
            acc
            (recur (inc k) (conj acc (if long-grid? (long p) p)))))))
    (let [unit (or every :day)
          [mult chrono] (or (and (keyword? unit)
                                 (some->> (math/canonical-unit tsfill-chrono-units unit)
                                          tsfill-chrono-units))
                            (throw (ex-info
                                    (str "tsfill: :every for a temporal date column must be one "
                                         "of :day :week :month :quarter :year (plural spellings "
                                         "accepted); got " (pr-str every) ".")
                                    {:dt/error :tsfill-invalid-every :dt/every every})))]
      (loop [k 0 acc []]
        (let [p (.plus ^java.time.temporal.Temporal lo (* (long mult) k) chrono)]
          (if (pos? (compare p hi))
            acc
            (recur (inc k) (conj acc p))))))))

(defn tsfill
  "Fill gaps in a panel: expand each group to a regular date grid, inserting
  missing rows (nil in every non-key column). Stata's `tsfill`; essentially
  tidyr's `complete` for panel time series. Follows tidyr's union semantics:
  existing rows are all KEPT (even off-grid ones) and grid rows are added
  where no exact date match exists — no data is ever dropped by gridding.

  Arguments:
    dataset - a tech.v3.dataset
    opts    - map with keys:
      :date  - (required) the date column keyword.
      :by    - group key column(s), keyword or vector. Omit to treat the whole
               dataset as one series.
      :every - grid step: a positive number for numeric date columns
               (default 1), or :day/:week/:month/:quarter/:year for temporal
               columns (default :day). Temporal grids are anchored at each
               group's min date; matching is exact, so panels keyed on
               month-end dates should be normalised first (e.g. an xbar
               bucket) — the same caveat as win/tlag.
      :grid  - explicit sequence of date values to use as the grid instead of
               :every (e.g. actual trading days); it is deduplicated, sorted,
               and clipped to each group's [min, max] date range.
      :carry - column(s) to carry across the inserted rows after expansion,
               per group: backward-fill then forward-fill (NOCB→LOCF,
               mbmisc `tsfill` carrycols) — e.g. a company name column.

  The grid spans each group's own min..max date (no rows are invented before
  a group's first or after its last observation). Rows whose date is nil are
  dropped (they cannot be placed on a grid); a group with only nil dates
  vanishes. nil group keys throw :tsfill-nil-key; duplicate (key, date)
  combinations throw :tsfill-duplicate-dates (aggregate duplicates first,
  e.g. :agg by the keys). Output rows are sorted by [keys…, date] — unlike
  :set, input order is not meaningful once rows are inserted.

  Example:
    (tsfill ds {:by [:gvkey] :date :fyear})                    ;; yearly panel
    (tsfill ds {:by [:permno] :date :month :every :month
                :carry [:ticker]})"
  [dataset {:keys [by date every grid carry]}]
  (when (nil? date)
    (throw (ex-info "tsfill: :date is required." {:dt/error :tsfill-missing-date})))
  (let [by (cond (nil? by) [] (keyword? by) [by] :else (vec by))
        carry (cond (nil? carry) [] (keyword? carry) [carry] :else (vec carry))
        available (set (ds/column-names dataset))
        unknown (vec (remove available (concat by [date] carry)))]
    (when (seq unknown)
      (throw (ex-info (str "tsfill: unknown column(s) " unknown ".")
                      {:dt/error :unknown-column :dt/columns unknown
                       :dt/available (vec (sort available))})))
    (if (zero? (ds/row-count dataset))
      dataset
      (let [n (ds/row-count dataset)
            key-rdrs (mapv #(dtype/->reader (ds/column dataset %)) by)
            date-rdr (dtype/->reader (ds/column dataset date))
            key-of (fn [v] (if (number? v) (double v) v))
            groups (reduce (fn [m i]
                             (let [k (mapv #(nth % i) key-rdrs)]
                               (when (some nil? k)
                                 (throw (ex-info
                                         (str "tsfill: nil group key in :by column(s) " by
                                              " at row " i " — group keys must be non-nil.")
                                         {:dt/error :tsfill-nil-key :dt/row i :dt/by by})))
                               (update m k (fnil conj []) i)))
                           {} (range n))
            ;; per group: [key date-value source-idx-or-nil] rows, grid-expanded
            group-rows
            (mapv (fn [k]
                    (let [idxs (groups k)
                          m (reduce (fn [acc i]
                                      (let [dv (nth date-rdr i)]
                                        (if (nil? dv)
                                          acc            ; nil dates dropped (documented)
                                          (let [kk (key-of dv)]
                                            (if (contains? acc kk)
                                              (throw (ex-info
                                                      (str "tsfill: duplicate date value " (pr-str dv)
                                                           " for group " (pr-str k) " — (key, date) must "
                                                           "be unique. Aggregate duplicates first "
                                                           "(e.g. :agg by the keys).")
                                                      {:dt/error :tsfill-duplicate-dates
                                                       :dt/date-value dv :dt/key k}))
                                              (assoc acc kk {:idx i :val dv}))))))
                                    {} idxs)]
                      (if (empty? m)
                        []
                        (let [ds-vals (sort compare (map :val (vals m)))
                              lo (first ds-vals)
                              hi (last ds-vals)
                              grid-vals (if (some? grid)
                                          (->> grid distinct
                                               (filter #(and (<= (compare lo %) 0)
                                                             (<= (compare % hi) 0))))
                                          (tsfill-grid lo hi every))
                              union (reduce (fn [acc gv]
                                              (let [kk (key-of gv)]
                                                (if (contains? acc kk)
                                                  acc
                                                  (assoc acc kk {:idx nil :val gv}))))
                                            m grid-vals)]
                          (mapv (fn [{:keys [idx val]}] [k val idx])
                                (sort-by :val compare (vals union)))))))
                  (sort compare (keys groups)))
            all-rows (into [] cat group-rows)
            total (count all-rows)
            group-bounds (loop [gs group-rows start 0 acc []]
                           (if-let [g (first gs)]
                             (recur (next gs) (+ start (count g))
                                    (conj acc [start (+ start (count g))]))
                             acc))
            by-set (set by)
            build-col (fn [c]
                        (cond
                          (= c date) (mapv second all-rows)
                          (by-set c) (let [j (.indexOf ^java.util.List by c)]
                                       (mapv #(nth (nth % 0) j) all-rows))
                          :else (let [rdr (dtype/->reader (ds/column dataset c))
                                      vals (mapv (fn [[_ _ idx]] (when idx (nth rdr idx)))
                                                 all-rows)]
                                  (if (some #{c} carry)
                                    ;; NOCB→LOCF per contiguous group slice
                                    (reduce (fn [vs [s e]]
                                              (let [slice (subvec vs s e)
                                                    filled (vec (win/win-fills (win/win-bfill slice)))]
                                                (into (into (subvec vs 0 s) filled) (subvec vs e total))))
                                            vals group-bounds)
                                    vals))))]
        (ds/new-dataset (mapv (fn [c] (ds/new-column c (build-col c)))
                              (ds/column-names dataset)))))))
