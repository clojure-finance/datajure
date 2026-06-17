(ns datajure.index
  "Keyed lookup indexes — explicit, immutable prepared row-index structures over
  a source dataset. An index is built once and reused for fast access; the source
  dataset is never reordered or mutated (this is data.table's `setindex()`, never
  `setkey()`), and the index carries a reference to the exact dataset value it was
  built from so it can never be applied to a mismatched table.

  Two kinds:

  - `:hash` (default) — equality point-lookups. All key columns form the lookup
    tuple. `lookup` resolves a key to that key's rows in O(1):

        (def by-tic (idx/index-by panel :tic))
        (idx/lookup by-tic \"AAPL\")            ;; => dataset of AAPL's rows
        (idx/lookup by-firm-date [1690 date])   ;; multi-column tuple key

  - `:asof` — the prepared right-side structure for as-of / window joins. The
    LAST key column is the asof column, the rest are exact-match keys; each exact
    tuple maps to its asof values (sorted ascending, nils last) plus original row
    ids. An `:asof` index is not point-lookable via `lookup` (the access pattern
    needs direction/tolerance/window semantics) — it is consumed by
    `datajure.asof` / a `:how :asof` join:

        (def right-idx (idx/index-by compustat [:gvkey :rdq] {:kind :asof}))
        (idx/asof-index compustat [:gvkey :rdq])   ;; convenience alias

  `lookup-indices` returns raw row indices for callers gathering from a row-aligned
  projection themselves."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]))

(def ^:private index-marker ::index)

;;; ---- key-column normalisation ----------------------------------------------

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

;;; ---- low-level row-key extraction (shared with datajure.asof) ---------------

(defn row-exact-key
  "Extract the exact-key tuple (all key columns except the last) from row `i`
  given `readers` (one reader per key column). Returns [] when there is only one
  key column (pure asof, no exact grouping). Low-level — shared with the as-of
  search layer for probing left rows."
  [readers ^long i]
  (let [n (dec (count readers))]
    (loop [j 0 acc (transient [])]
      (if (= j n)
        (persistent! acc)
        (recur (inc j) (conj! acc (nth (nth readers j) i)))))))

(defn row-asof-val
  "Extract the asof-key value (last key column) from row `i`. Low-level."
  [readers ^long i]
  (nth (peek readers) i))

;;; ---- table builders --------------------------------------------------------

(defn- build-hash-table
  "Map equality-key -> vector of row indices (ascending). Single-column keys use
  the bare scalar value; multi-column keys use a tuple."
  [dataset kcols n]
  (if (= 1 (count kcols))
    (let [rdr (dtype/->reader (ds/column dataset (first kcols)))]
      (group-by #(nth rdr %) (range n)))
    (let [rdrs (mapv #(dtype/->reader (ds/column dataset %)) kcols)]
      (group-by (fn [i] (mapv #(nth % i) rdrs)) (range n)))))

(defn- build-asof-table
  "Map exact-key-tuple -> {:reader <reader over the asof-vals, sorted ascending,
  nils last> :orig <vector of original row indices, parallel to :reader> :n}.
  Right dataset need not be pre-sorted — sorting happens here. asof-vals/orig are
  split apart and the reader wrapped ONCE per group, so per-left-row probing does
  no re-splitting (O(log group-size) per probe rather than O(group-size))."
  [dataset kcols n]
  (let [readers (mapv #(dtype/->reader (ds/column dataset %)) kcols)
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
     (fn [m k pairs]
       (let [sorted (sort-by first asof-cmp pairs)
             avals (mapv first sorted)]
         (assoc m k {:reader (dtype/->reader avals)
                     :orig (mapv second sorted)
                     :n (count avals)})))
     {}
     grouped)))

;;; ---- construction ----------------------------------------------------------

(defn index-by
  "Build an immutable index over `dataset` keyed by `key-cols` (a keyword or a
  vector of keywords). `opts` may set `:kind` to `:hash` (default) or `:asof`.

  `:hash` — all key columns form the equality tuple; rows are grouped by key, so a
  key may map to many rows (ascending original order). O(row-count) to build,
  O(1) per `lookup`.

  `:asof` — the last key column is the asof column, the rest are exact-match keys;
  the prepared structure for `datajure.asof` (see ns doc). Consumed by as-of /
  window joins, not by `lookup`.

  The index holds a reference to `dataset`."
  ([dataset key-cols] (index-by dataset key-cols {}))
  ([dataset key-cols opts]
   (let [kcols (->key-cols key-cols)
         kind (get opts :kind :hash)]
     (validate-key-cols dataset kcols)
     (let [n (ds/row-count dataset)
           table (case kind
                   :hash (build-hash-table dataset kcols n)
                   :asof (build-asof-table dataset kcols n)
                   (throw (ex-info (str "index-by: unknown :kind " (pr-str kind)
                                        ". Must be :hash or :asof.")
                                   {:dt/error :invalid-index-kind :dt/kind kind})))]
       {:datajure.index/marker index-marker
        :datajure.index/kind kind
        :datajure.index/dataset dataset
        :datajure.index/key-cols kcols
        :datajure.index/table table}))))

(defn asof-index
  "Convenience for `(index-by dataset key-cols {:kind :asof})`."
  [dataset key-cols]
  (index-by dataset key-cols {:kind :asof}))

;;; ---- predicates / introspection --------------------------------------------

(defn index?
  "True if `x` is a datajure index value."
  [x]
  (and (map? x) (= index-marker (:datajure.index/marker x))))

(defn kind
  "The kind of `index` — `:hash` or `:asof`."
  [index]
  (:datajure.index/kind index))

(defn key-columns
  "The vector of key columns this index is built on."
  [index]
  (:datajure.index/key-cols index))

(defn source-dataset
  "The dataset value this index was built from."
  [index]
  (:datajure.index/dataset index))

(defn asof-groups
  "Low-level: the exact-key -> {:reader :orig :n} table of an `:asof` index.
  Used by `datajure.asof` to consume a prebuilt index."
  [index]
  (:datajure.index/table index))

;;; ---- lookup (hash kind) ----------------------------------------------------

(defn- ensure-index [index]
  (when-not (index? index)
    (throw (ex-info (str "expected a datajure index (from index-by), got: " (pr-str index))
                    {:dt/error :not-an-index :dt/value index}))))

(defn- ensure-hash [index]
  (when (= :asof (kind index))
    (throw (ex-info (str "lookup/lookup-indices is for :hash indexes; an :asof index "
                         "is consumed through datajure.asof / a :how :asof join.")
                    {:dt/error :asof-index-not-lookable
                     :dt/key-cols (key-columns index)}))))

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

(defn lookup-indices
  "Return the vector of row indices (into the index's source dataset) whose key
  equals `k` — a scalar for a single-column index, a tuple/vector for a
  multi-column one. Empty vector if the key is absent. `:hash` indexes only."
  [index k]
  (ensure-index index)
  (ensure-hash index)
  (-> (:datajure.index/table index)
      (get (normalize-key (:datajure.index/key-cols index) k))
      (or [])))

(defn lookup
  "Return the rows of the index's source dataset whose key equals `k`, as a
  dataset in original row order. Empty (0-row) dataset if the key is absent.
  `:hash` indexes only."
  [index k]
  (ensure-index index)
  (ds/select-rows (:datajure.index/dataset index) (lookup-indices index k)))
