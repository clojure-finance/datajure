(ns datajure.reshape
  "Reshape functions for datajure: wide->long (melt) and long->wide (cast)."
  (:refer-clojure :exclude [cast])
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]))

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
      :id    - (required) vector of column keywords to use as row identifiers
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

    ;; With aggregation for duplicate cells
    (cast ds {:id [:date :sym] :from :metric :value :val :agg dfn/mean})"
  [dataset {:keys [id from value agg fill]
            :or {fill nil}}]
  (when-not id (throw (ex-info "cast requires :id" {:dt/error :cast-missing-id})))
  (when-not from (throw (ex-info "cast requires :from" {:dt/error :cast-missing-from})))
  (when-not value (throw (ex-info "cast requires :value" {:dt/error :cast-missing-value})))
  (let [id-rdrs (mapv #(dtype/->reader (ds/column dataset %)) id)
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
