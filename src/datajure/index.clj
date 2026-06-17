(ns datajure.index
  "Keyed lookup index for repeated point-lookups into a dataset.

  An *index* is an explicit, immutable value built once from a dataset and a set
  of key columns; `lookup` then resolves a key to that key's rows in O(1) instead
  of scanning the dataset. This is the value-oriented analogue of data.table's
  `setindex()` (a secondary index) — never `setkey()`: the source dataset is not
  reordered or mutated. The index carries a reference to the exact dataset value
  it was built from, so a lookup can never be applied to a mismatched table.

  Typical use — pull one firm's rows out of a wide panel for a UI:

      (def by-tic (idx/index-by panel :tic))
      (idx/lookup by-tic \"AAPL\")          ;; => dataset of AAPL's rows, original order

  Multi-column keys use a tuple:

      (def by-firm-date (idx/index-by panel [:gvkey :datadate]))
      (idx/lookup by-firm-date [1690 (java.time.LocalDate/parse \"2020-03-31\")])

  `lookup-indices` returns the raw row indices instead of a sub-dataset, for
  callers that want to gather from a row-aligned projection themselves."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]))

(def ^:private index-marker ::index)

;;; ---- construction ----------------------------------------------------------

(defn- ->key-cols
  "Normalise the key-columns argument to a vector of keywords."
  [key-cols]
  (cond
    (keyword? key-cols) [key-cols]
    (sequential? key-cols) (vec key-cols)
    :else (throw (ex-info (str "index-by: key columns must be a keyword or a vector "
                               "of keywords, got: " (pr-str key-cols))
                          {:dt/error :invalid-key-cols :dt/value key-cols}))))

(defn- validate-key-cols [dataset kcols]
  (when (empty? kcols)
    (throw (ex-info "index-by: at least one key column is required."
                    {:dt/error :invalid-key-cols :dt/value kcols})))
  (let [present (set (ds/column-names dataset))
        missing (remove present kcols)]
    (when (seq missing)
      (throw (ex-info (str "index-by: unknown key column(s): " (vec missing)
                           ". Available: " (vec (ds/column-names dataset)))
                      {:dt/error :unknown-column
                       :dt/unknown (vec missing)
                       :dt/available (vec (ds/column-names dataset))})))))

(defn index-by
  "Build an immutable lookup index over `dataset` keyed by `key-cols` (a keyword
  or a vector of keywords). Returns an index value to pass to `lookup` /
  `lookup-indices`. Rows are grouped by key, so a key may map to many rows;
  within a key, indices stay in original (ascending) row order. O(row-count) to
  build, O(1) per later lookup. The index holds a reference to `dataset`."
  [dataset key-cols]
  (let [kcols (->key-cols key-cols)]
    (validate-key-cols dataset kcols)
    (let [n (ds/row-count dataset)
          table (if (= 1 (count kcols))
                  (let [rdr (dtype/->reader (ds/column dataset (first kcols)))]
                    (group-by #(nth rdr %) (range n)))
                  (let [rdrs (mapv #(dtype/->reader (ds/column dataset %)) kcols)]
                    (group-by (fn [i] (mapv #(nth % i) rdrs)) (range n))))]
      {:datajure.index/marker index-marker
       :datajure.index/dataset dataset
       :datajure.index/key-cols kcols
       :datajure.index/table table})))

;;; ---- predicates / introspection --------------------------------------------

(defn index?
  "True if `x` is a datajure index value."
  [x]
  (and (map? x) (= index-marker (:datajure.index/marker x))))

(defn key-columns
  "The vector of key columns this index is built on."
  [index]
  (:datajure.index/key-cols index))

(defn- ensure-index [index]
  (when-not (index? index)
    (throw (ex-info (str "expected a datajure index (from index-by), got: " (pr-str index))
                    {:dt/error :not-an-index :dt/value index}))))

(defn- normalize-key
  "Coerce a lookup key to the form stored in the table: a scalar for a
  single-column index, a vector tuple for a multi-column one."
  [kcols k]
  (if (= 1 (count kcols))
    (if (and (sequential? k) (= 1 (count k))) (first k) k)
    (if (sequential? k)
      (vec k)
      (throw (ex-info (str "lookup: index on " kcols " expects a "
                           (count kcols) "-element key tuple, got: " (pr-str k))
                      {:dt/error :invalid-lookup-key
                       :dt/key-cols kcols :dt/value k})))))

;;; ---- lookup ----------------------------------------------------------------

(defn lookup-indices
  "Return the vector of row indices (into the index's source dataset) whose key
  equals `k` — a scalar for a single-column index, a tuple/vector for a
  multi-column one. Empty vector if the key is absent. Use this when you want to
  gather from a row-aligned projection of the dataset yourself."
  [index k]
  (ensure-index index)
  (-> (:datajure.index/table index)
      (get (normalize-key (:datajure.index/key-cols index) k))
      (or [])))

(defn lookup
  "Return the rows of the index's source dataset whose key equals `k`, as a
  dataset in original row order. Empty (0-row) dataset if the key is absent."
  [index k]
  (ensure-index index)
  (ds/select-rows (:datajure.index/dataset index) (lookup-indices index k)))
