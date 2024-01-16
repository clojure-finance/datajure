(ns {{namespace}}.core
  (:require
   [datajure.dsl :as dtj]))

(defn -main
  []
  (dtj/set-backend "tech.ml.dataset")
  (def data {:age [31 25 18 18 25]
             :name ["a" "b" "c" "c" "d"]
             :salary [200 500 200 370 3500]})
  (-> data
      (dtj/dataset)
      (dtj/query [[:salary #(< 300 %)] [:age #(> 20 %)]] [])
      (dtj/print)))
