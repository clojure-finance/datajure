(ns dp.dsl)

(require '[dp.ds_opertation :as op])


(def operation-list [:where :row :group-by :having :select])

(def operation-function-map
  {:select op/select
   :where op/where
   :row op/row
   :group-by op/group-by
   :having op/having})

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
  (generic-operation-r dataset query-map operation-list))


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
        group-by-list (into [] (second (rest partition-exp)))
        query-map {:row row-list
                   :select select-list
                   :where where-list
                   :group-by group-by-list
                   :having having-list}]
    `(query-using-map ~dataset ~query-map)))



(require '[tech.v3.dataset :as ds]
         '[tech.v3.datatype :as dtype])

(def data (ds/->dataset {:age [31 25 24 24 25]
                         :name ["a" "b" "a" "a" "b"]
                         :salary [200 500 200 nil 3500]}))

(defn -main
  "Testing Funciton for DSL"
  [& args]

  (println (dt-get data '[[:sum :salary #(< 0 %)] [:age #(< 0 %)] & :name :age :salary :sum :salary :sd :salary & :name :age])))
