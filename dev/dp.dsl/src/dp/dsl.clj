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
  [& get-expression]
  (let [dataset (first get-expression)
        expression (second (first (rest get-expression)))
        partition-exp (remove #(= '(&) %) (partition-by #(= '& %) expression))
        row-filter-list (first partition-exp)
        row-list (filterv #(number? %) row-filter-list)
        filter-list (filterv #(not (number? %)) row-filter-list)
        where-list (filterv #(= 2 (count %)) filter-list)
        having-list (filterv #(= 3 (count %)) filter-list)
        select-list (into [] (second partition-exp))
        options-map (get-optional-exp-partition-map (second (rest partition-exp)))
        ;group-by-list (into [] (second (rest partition-exp)))
        group-by-list (get options-map :group-by)
        sort-by-list (get options-map :sort-by)

        query-map {:row row-list
                   :select select-list
                   :where where-list
                   :group-by group-by-list
                   :having having-list
                   :sort-by sort-by-list}]
    `(query-using-map ~dataset ~query-map)))







(def data (ds/->dataset {:age [31 25 24 24 25]
                         :name ["a" "b" "a" "a" "b"]
                         :salary [200 500 200 nil 3500]}))

(defn -main
  "Testing Funciton for DSL"
  [& args]

  (println (dt-get data '[[:sum :salary #(< 0 %)] [:age #(< 0 %)] & :name :age :salary :sum :salary :sd :salary & :group-by :name :age]))
  (println (dt-get data '[[:salary #(< 0 %)] [:age #(< 24 %)] & :name :*]))
  (println (dt-get data '[[:sum :salary #(< 0 %)] [:age #(< 0 %)] & :name :age :salary :sum :salary :sd :salary & :group-by :name :age :sort-by :salary]))
  )
