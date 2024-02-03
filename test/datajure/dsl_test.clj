(ns datajure.dsl-test
  (:require [clojask.dataframe :as ck]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [datajure.dsl :as dtj]
            [datajure.operation-ck :as op-ck]
            [zero-one.geni.core :as g]))

(def data {:age [31 25 18 18 25]
           :name ["a" "b" "c" "c" "d"]
           :salary [200 500 200 370 3500]})

(defn- check
  [s t]
  (= (str/split-lines s)
     (str/split-lines t)))

(deftest ds-test
  (let [expected (slurp "./test/datajure/ds-expected.txt")
        actual (with-out-str (do (dtj/set-backend "tech.ml.dataset")
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:salary #(< 300 %)] [:age #(> 20 %)]] [])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:sum :salary #(< 1000 %)]] [:age :sum :salary] [:group-by :age])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [] [:age :sum :salary :sd :salary] [:group-by :age :sort-by :sd :salary >])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [] [:age :name :sum :salary] [:group-by :age :name])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:salary #(< 0 %)] [:age #(< 24 %)]] [])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:sum :salary #(< 0 %)] [:age #(< 0 %)]] [:name :age :salary :sum :salary :sd :salary] [:group-by :name :age :sort-by :salary])
                                     (dtj/print))))]
    (is (check actual expected) actual)))

(deftest tc-test
  (let [expected (slurp "./test/datajure/tc-expected.txt")
        actual (with-out-str (do (dtj/set-backend "tablecloth")
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:salary #(< 300 %)] [:age #(> 20 %)]] [])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:sum :salary #(< 1000 %)]] [:age :sum :salary] [:group-by :age])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [] [:age :sum :salary :sd :salary] [:group-by :age :sort-by :sd :salary >])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [] [:age :name :sum :salary] [:group-by :age :name])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:salary #(< 0 %)] [:age #(< 24 %)]] [])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:sum :salary #(< 0 %)] [:age #(< 0 %)]] [:name :age :salary :sum :salary :sd :salary] [:group-by :name :age :sort-by :salary])
                                     (dtj/print))))]
    (is (check actual expected) actual)))

(deftest ck-test
  (dtj/set-backend "clojask")
  (let [expected (slurp "./test/datajure/ck-create-expected.txt")
        actual (with-out-str (-> data
                                 (dtj/dataset)
                                 (dtj/print)))]
    (is (check actual expected) actual))
  (let [expected (slurp "./test/datajure/ck-create-expected.txt")
        actual (with-out-str (-> "./test/datajure/ck-input.txt"
                                 (ck/dataframe)
                                 (ck/set-parser "salary" #(Long/parseLong %))
                                 (ck/set-parser "age" #(Long/parseLong %))
                                 (dtj/print)))]
    (is (check actual expected) actual))
  (let [input "./test/datajure/ck-sort-input.txt"
        output "./test/datajure/ck-sort-output.txt"
        expected (slurp "./test/datajure/ck-sort-expected.txt")
        actual (do (op-ck/external-sort input output #(- (Integer/parseInt (.get %1 "salary")) (Integer/parseInt (.get %2 "salary"))))
                   (slurp output))]
    (is (check actual expected) actual))
  (let [expected (slurp "./test/datajure/ck-where-expected.txt")
        actual (with-out-str (-> data
                                 (dtj/dataset)
                                 (op-ck/where {:where [[:salary #(< 300 %)] [:age #(> 20 %)]]})
                                 (dtj/print)))]
    (is (check actual expected) actual)))

(deftest g-test
  (let [expected (slurp "./test/datajure/g-expected.txt")
        actual (with-out-str (do (dtj/set-backend "geni")
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:salary (g/< (g/lit 300) :salary)] [:age (g/> (g/lit 20) :age)]] [])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:sum :salary (g/< (g/lit 1000) (keyword "sum(salary)"))]] [:age :sum :salary] [:group-by :age])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [] [:age :sum :salary :sd :salary] [:group-by :age :sort-by :sd :salary])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [] [:age :name :sum :salary] [:group-by :age :name :sort-by :sum :salary])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:salary (g/< (g/lit 0) :salary)] [:age (g/< (g/lit 24) :age)]] [] [:sort-by :salary])
                                     (dtj/print))
                                 (-> data
                                     (dtj/dataset)
                                     (dtj/query [[:sum :salary (g/< (g/lit 0) (keyword "sum(salary)"))] [:age (g/< (g/lit 0) :age)]] [:name :age :salary :sum :salary :sd :salary] [:group-by :name :age :sort-by :sum :salary])
                                     (dtj/print))))]
    (is (check actual expected) actual)))