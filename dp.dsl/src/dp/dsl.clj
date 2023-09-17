(ns dp.dsl)

(require '[tech.v3.dataset :as ds]
         '[tablecloth.api :as tc]
         '[clojask.dataframe :as ck]
         '[zero-one.geni.core :as g]
         '[dp.ds-operation :as op]
         '[dp.ds-operation-tc :as op-tc]
         '[dp.ds-operation-ck :as op-ck]
         '[dp.ds-operation-g :as op-g])

(def operation-list [:where :row :group-by :having :select :sort-by])
(def optional-keywords #{:group-by :sort-by})

(def backend (atom "tablecloth"))

(def operation-function-map
  (case @backend
    "tech.v3.dataset" {:select op/select
                       :where op/where
                       :row op/row
                       :group-by op/group-by
                       :having op/having
                       :sort-by op/sort-by}
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
  [dataset query-map operation]
  ((get operation-function-map operation) dataset query-map))

(defn- generic-operation-r
  [dataset query-map operations]
  (reduce #(apply-generic-operation %1 query-map %2) dataset operations))

(defn- get-col-names
  [dataset]
  (case @backend
    "tech.v3.dataset" (ds/column-names dataset)
    "tablecloth" (tc/column-names dataset)
    "clojask" (ck/get-col-names dataset)
    "geni" (g/columns dataset)))

(defn query-using-map
  [dataset query-map]
  (generic-operation-r dataset (assoc query-map :original-col (into [] (get-col-names dataset))) operation-list))

(defn- partition-with
  [f coll]
  (lazy-seq
   (when-let [s (seq coll)]
     (let [run (cons (first s) (take-while (complement f) (next s)))]
       (cons run (partition-with f (seq (drop (count run) s))))))))

(defn- get-optional-exp-partition-map
  [optional-exp]
  (if (nil? optional-exp)
    {:group-by []
     :sort-by []}
    (let [partition-exp (partition-with #(contains? optional-keywords %) optional-exp)
          partition-exp-listed (mapv #(vector (first %) (into (vector) (rest %))) partition-exp)]
      (into (sorted-map) partition-exp-listed))))

(defmacro dt-get
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
   `(dt-get ~dataset ~row-filter-list ~select-list [])))

(defn dataset
  [data]
  (case @backend
    "tech.v3.dataset" (ds/->dataset data)
    "tablecloth" (tc/dataset data)
    "clojask" (ck/dataframe #(op-ck/ck-transform data))
    "geni" (g/map->dataset data)))

(defn print-dataset
  [data]
  (case @backend
    "tech.v3.dataset" (println data)
    "tablecloth" (println data)
    "clojask" (ck/print-df data)
    "geni" (g/show data)))

(defn set-backend
  [back]
  (reset! backend back))

(defn -main
  "Testing Funciton for DSL"
  [& args]
  (set-backend "tablecloth")
  (def data {:age [31 25 18 18 25]
             :name ["a" "b" "c" "c" "d"]
             :salary [200 500 200 370 3500]})
  (print-dataset (dt-get (dataset data) [[:salary #(< 300 %)] [:age #(> 20 %)]] []))
  (print-dataset (dt-get (dataset data) [[:sum :salary #(< 1000 %)]] [:age :sum :salary] [:group-by :age]))
  (print-dataset (dt-get (dataset data) [] [:age :sum :salary :sd :salary] [:group-by :age :sort-by :sd :salary >]))
  (print-dataset (dt-get (dataset data) [] [:age :name :sum :salary] [:group-by :age :name]))
  (print-dataset (dt-get (dataset data) [[:salary #(< 0 %)] [:age #(< 24 %)]] []))
  (print-dataset (dt-get (dataset data) [[:sum :salary #(< 0 %)] [:age #(< 0 %)]] [:name :age :salary :sum :salary :sd :salary] [:group-by :name :age :sort-by :salary])))
