(ns datajure.reshape
  "Reshape functions for datajure. Currently provides wide->long (melt)."
  (:require [tech.v3.dataset :as ds]))

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
