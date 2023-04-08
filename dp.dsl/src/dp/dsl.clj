(ns dp.dsl)

(require '[tech.v3.dataset :as ds]
         '[dp.ds_operation :as op])


(def operation-list [:where :row :group-by :having :select :sort-by])
(def optional-keywords #{:group-by :sort-by})

(def operation-function-map
  {:select op/select
   :where op/where
   :row op/row
   :group-by op/group-by
   :having op/having
   :sort-by op/sort-by})

(defn- apply-generic-operation
  [dataset query-map operation]
  ((get operation-function-map operation) dataset query-map))

(defn- generic-operation-r
  [dataset query-map operations]
  (if (empty? operations)
    dataset
    (-> dataset
        (apply-generic-operation query-map (first operations))
        (generic-operation-r query-map (rest operations)))))

(defn query-using-map
  [dataset query-map]
  (generic-operation-r dataset (assoc query-map :original-col (into [] (ds/column-names dataset))) operation-list))

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
         where-list (if (.contains filter-list :*) [:*] (filterv #(= 2 (count %)) filter-list))
         having-list (->> filter-list
                          (filterv vector?)
                          (filterv #(= 3 (count %))))
         group-by-list (:group-by options-map)
         sort-by-list (:sort-by options-map)

         query-map {:row row-list
                    :select select-list
                    :where where-list
                    :group-by group-by-list
                    :having having-list
                    :sort-by sort-by-list}]
    `(query-using-map ~dataset ~query-map)))
  ([dataset row-filter-list select-list]
   `(dt-get ~dataset ~row-filter-list ~select-list [])))




(def data (ds/->dataset {:age [31 25 18 18 25]
                         :name ["a" "b" "c" "c" "d"]
                         :salary [200 500 200 370 3500]}))

(defn -main
  "Testing Funciton for DSL"
  [& args]
  (println (dt-get data [[:salary #(< 300 %)] [:age #(> 20 %)]] [:*]))
  (println (dt-get data [[:sum :salary #(< 1000 %)]] [:age :sum :salary] [:group-by :age]))
  (println (dt-get data [:*] [:age :sum :salary :sd :salary] [:group-by :age :sort-by :sd :salary >]))
  (println (dt-get data [:*] [:age :name :sum :salary] [:group-by :age :name]))
  (println (dt-get data [[:salary #(< 0 %)] [:age #(< 24 %)]] [:name :*]))
  (println (dt-get data [[:sum :salary #(< 0 %)] [:age #(< 0 %)]] [:name :age :salary :sum :salary :sd :salary] [:group-by :name :age :sort-by :salary])))
