(ns datajure.dsl)

(require '[tech.v3.dataset :as ds]
         '[tablecloth.api :as tc]
         '[clojask.dataframe :as ck]
         '[zero-one.geni.core :as g]
         '[datajure.operation-ds :as op-ds]
         '[datajure.operation-tc :as op-tc]
         '[datajure.operation-ck :as op-ck]
         '[datajure.operation-g :as op-g])

(def ^:private operation-list [:where :row :group-by :having :select :sort-by])
(def ^:private optional-keywords #{:group-by :sort-by})

(def ^:private backend (atom "tablecloth"))

(defn- get-operation-function-map
  "Return the operation function map according to the current backend."
  []
  (case @backend
    "tech.v3.dataset" {:select op-ds/select
                       :where op-ds/where
                       :row op-ds/row
                       :group-by op-ds/group-by
                       :having op-ds/having
                       :sort-by op-ds/sort-by}
    "tablecloth" {:select op-tc/select
                  :where op-tc/where
                  :row op-tc/row
                  :group-by op-tc/group-by
                  :having op-tc/having
                  :sort-by op-tc/sort-by}
    "clojask" {:select op-ck/select
               :where op-ck/where
               :row op-ck/row
               :group-by op-ck/group-by
               :having op-ck/having
               :sort-by op-ck/sort-by}
    "geni" {:select op-g/select
            :where op-g/where
            :row op-g/row
            :group-by op-g/group-by
            :having op-g/having
            :sort-by op-g/sort-by}))

(defn- apply-generic-operation
  "Perform the query actions on `dataset` according to `query-map` and `operations`."
  [dataset query-map operation]
  ((get (get-operation-function-map) operation) dataset query-map))

(defn- generic-operation-r
  "Perform the query actions on `dataset` according to `query-map` and `operations`."
  [dataset query-map operations]
  (reduce #(apply-generic-operation %1 query-map %2) dataset operations))

(defn- get-col-names
  "Get the column names of `dataset`."
  [dataset]
  (case @backend
    "tech.v3.dataset" (ds/column-names dataset)
    "tablecloth" (tc/column-names dataset)
    "clojask" (ck/get-col-names dataset)
    "geni" (g/columns dataset)))

(defn query-using-map
  "Perform the query actions on `dataset` according to `query-map`."
  [dataset query-map]
  (generic-operation-r dataset (assoc query-map :original-col (into [] (get-col-names dataset))) operation-list))

(defn- partition-with
  "Partite the collection `coll` according to the given function `f`."
  [f coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (let [run (cons (first s) (take-while (complement f) (next s)))]
       (cons run (partition-with f (seq (drop (count run) s))))))))

(defn- get-optional-exp-partition-map
  "Re-organize the structure of `optional-exp` and return a map containing a `:group-by` field and a `:sort-by` field."
  [optional-exp]
  (if (nil? optional-exp)
    {:group-by []
     :sort-by []}
    (let [partition-exp (partition-with #(contains? optional-keywords %) optional-exp)
          partition-exp-listed (mapv #(vector (first %) (into (vector) (rest %))) partition-exp)]
      (into (sorted-map) partition-exp-listed))))

(defmacro query
  "Generate `query-map` from Datajure DSL (`row-filter-list`, `select-list`, and `options-map`). Then perform the data operations to `dataset`."
  ([dataset row-filter-list select-list options-map]
   (let [options-map (get-optional-exp-partition-map options-map)
         {row-list true filter-list false} (group-by number? row-filter-list)
         where-list (filterv #(= 2 (count %)) filter-list)
         having-list (->> filter-list
                          (filterv vector?)
                          (filterv #(= 3 (count %))))
         group-by-list (:group-by options-map)
         sort-by-list (:sort-by options-map)

         query-map {:row row-list
                    :where where-list
                    :group-by group-by-list
                    :having having-list
                    :sort-by sort-by-list
                    :select select-list}]
    `(query-using-map ~dataset ~query-map)))
  ([dataset row-filter-list select-list]
   `(query ~dataset ~row-filter-list ~select-list [])))

(defn dataset
  "Create and return a dataset object from an associative map `data`."
  [data]
  (case @backend
    "tech.v3.dataset" (ds/->dataset data)
    "tablecloth" (tc/dataset data)
    "clojask" (ck/dataframe #(op-ck/ck-transform data))
    "geni" (g/map->dataset data)))

(defn print
  "Print the dataset `data`."
  [data]
  (case @backend
    "tech.v3.dataset" (println data)
    "tablecloth" (println data)
    "clojask" (ck/print-df data)
    "geni" (g/show data)))

(defn set-backend
  "Choose `back` as the backend implementation."
  [back]
  (reset! backend back))
