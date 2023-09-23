(ns dp.dsl-test
  (:require [clojure.test :refer :all]
            [dp.dsl :refer :all]
            [zero-one.geni.core :as g]))

(def data {:age [31 25 18 18 25]
           :name ["a" "b" "c" "c" "d"]
           :salary [200 500 200 370 3500]})

(deftest ds-test
  (set-backend "tech.v3.dataset")
  (print-dataset (dt-get (dataset data) [[:salary #(< 300 %)] [:age #(> 20 %)]] []))
  (print-dataset (dt-get (dataset data) [[:sum :salary #(< 1000 %)]] [:age :sum :salary] [:group-by :age]))
  (print-dataset (dt-get (dataset data) [] [:age :sum :salary :sd :salary] [:group-by :age :sort-by :sd :salary >]))
  (print-dataset (dt-get (dataset data) [] [:age :name :sum :salary] [:group-by :age :name]))
  (print-dataset (dt-get (dataset data) [[:salary #(< 0 %)] [:age #(< 24 %)]] []))
  (print-dataset (dt-get (dataset data) [[:sum :salary #(< 0 %)] [:age #(< 0 %)]] [:name :age :salary :sum :salary :sd :salary] [:group-by :name :age :sort-by :salary])))

(deftest tc-test
  (set-backend "tablecloth")
  (print-dataset (dt-get (dataset data) [[:salary #(< 300 %)] [:age #(> 20 %)]] []))
  (print-dataset (dt-get (dataset data) [[:sum :salary #(< 1000 %)]] [:age :sum :salary] [:group-by :age]))
  (print-dataset (dt-get (dataset data) [] [:age :sum :salary :sd :salary] [:group-by :age :sort-by :sd :salary >]))
  (print-dataset (dt-get (dataset data) [] [:age :name :sum :salary] [:group-by :age :name]))
  (print-dataset (dt-get (dataset data) [[:salary #(< 0 %)] [:age #(< 24 %)]] []))
  (print-dataset (dt-get (dataset data) [[:sum :salary #(< 0 %)] [:age #(< 0 %)]] [:name :age :salary :sum :salary :sd :salary] [:group-by :name :age :sort-by :salary])))

(deftest g-test
  (set-backend "geni")
  (print-dataset (dt-get (dataset data) [[:salary (g/< (g/lit 300) :salary)] [:age (g/> (g/lit 20) :age)]] []))
  (print-dataset (dt-get (dataset data) [[:sum :salary (g/< (g/lit 1000) (keyword "sum(salary)"))]] [:age :sum :salary] [:group-by :age]))
  (print-dataset (dt-get (dataset data) [] [:age :sum :salary :sd :salary] [:group-by :age :sort-by :sd :salary]))
  (print-dataset (dt-get (dataset data) [] [:age :name :sum :salary] [:group-by :age :name]))
  (print-dataset (dt-get (dataset data) [[:salary (g/< (g/lit 0) :salary)] [:age (g/< (g/lit 24) :age)]] []))
  (print-dataset (dt-get (dataset data) [[:sum :salary (g/< (g/lit 0) (keyword "sum(salary)"))] [:age (g/< (g/lit 0) :age)]] [:name :age :salary :sum :salary :sd :salary] [:group-by :name :age :sort-by :salary])))