(ns datajure.util-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [datajure.util :as du]))

(deftest describe-all-columns
  (let [ds (ds/->dataset {:name ["Alice" "Bob" "Charlie"]
                          :age [30 25 35]
                          :score [85.5 92.0 78.3]})
        result (du/describe ds)]
    (testing "returns a dataset with one row per column"
      (is (ds/dataset? result))
      (is (= 3 (ds/row-count result))))
    (testing "has expected stat columns"
      (is (every? #(contains? (set (ds/column-names result)) %)
                  [:column :datatype :n :n-missing :mean :sd :min :p25 :median :p75 :max])))
    (testing "numeric columns have stats"
      (let [age-row (first (filter #(= :age (:column %)) (ds/mapseq-reader result)))]
        (is (= 3 (:n age-row)))
        (is (= 0 (:n-missing age-row)))
        (is (= 30.0 (:mean age-row)))
        (is (= 25.0 (:min age-row)))
        (is (= 35.0 (:max age-row)))))
    (testing "non-numeric columns have nil stats"
      (let [name-row (first (filter #(= :name (:column %)) (ds/mapseq-reader result)))]
        (is (= 3 (:n name-row)))
        (is (nil? (:mean name-row)))
        (is (nil? (:sd name-row)))))))

(deftest describe-column-subset
  (let [ds (ds/->dataset {:a [1 2 3] :b [4 5 6] :c ["x" "y" "z"]})
        result (du/describe ds [:a :b])]
    (is (= 2 (ds/row-count result)))
    (is (= #{:a :b} (set (map :column (ds/mapseq-reader result)))))))

(deftest describe-with-missing
  (let [ds (ds/->dataset {:x [1 nil 3 nil 5]})
        result (du/describe ds [:x])
        row (first (ds/mapseq-reader result))]
    (is (= 5 (:n row)))
    (is (= 2 (:n-missing row)))))

(deftest describe-all-missing-numeric
  ;; Regression: an explicitly :float64 column of all-NaN (all-missing) used to
  ;; return :sd = -0.0 while :mean/:min/:max/:median/percentiles correctly
  ;; returned nil, giving an incoherent summary row. With the guard all stats
  ;; go through the nil branch.
  (testing "all-missing :float64 column — every stat is nil"
    (let [col (dtype/make-container :float64 [##NaN ##NaN ##NaN])
          ds (ds/->dataset {:name ["a" "b" "c"] :x col})
          result (du/describe ds)
          row (first (filter #(= :x (:column %)) (ds/mapseq-reader result)))]
      (is (= 3 (:n row)))
      (is (= 3 (:n-missing row)))
      (is (= :float64 (:datatype row)))
      (is (nil? (:mean row)))
      (is (nil? (:sd row)) ":sd was the regression — must be nil, not -0.0")
      (is (nil? (:min row)))
      (is (nil? (:max row)))
      (is (nil? (:median row)))
      (is (nil? (:p25 row)))
      (is (nil? (:p75 row)))))
  (testing "empty numeric column — every stat is nil"
    (let [col (dtype/make-container :float64 [])
          ds (ds/->dataset {:x col})
          result (du/describe ds)
          row (first (ds/mapseq-reader result))]
      (is (= 0 (:n row)))
      (is (nil? (:mean row)))
      (is (nil? (:sd row)))))
  (testing "regression: non-all-missing numeric column still computes stats"
    (let [ds (ds/->dataset {:x [1.0 2.0 3.0 4.0 5.0]})
          row (first (ds/mapseq-reader (du/describe ds)))]
      (is (= 3.0 (:mean row)))
      (is (some? (:sd row)))
      (is (= 1.0 (:min row)))
      (is (= 5.0 (:max row))))))

(deftest clean-column-names-basic
  (let [ds (ds/->dataset {"Some Ugly Name!" [1] "Revenue ($)" [100] "year" [2024]})
        result (du/clean-column-names ds)]
    (is (= #{:some-ugly-name :revenue :year} (set (ds/column-names result))))
    (is (= 1 (ds/row-count result)))))

(deftest clean-column-names-special-chars
  (let [ds (ds/->dataset {"a--b" [1] "  leading  " [2] "UPPER" [3]})
        result (du/clean-column-names ds)]
    (is (contains? (set (ds/column-names result)) :a-b))
    (is (contains? (set (ds/column-names result)) :leading))
    (is (contains? (set (ds/column-names result)) :upper))))

(deftest clean-column-names-unicode
  (testing "preserves CJK characters while stripping punctuation"
    (let [ds (ds/->dataset {"市值 (HKD millions)!" [100]
                            "公司 Name" ["A"]
                            "Stock Code" ["0700.HK"]})
          result (du/clean-column-names ds)
          names (set (ds/column-names result))]
      (is (contains? names :市值-hkd-millions))
      (is (contains? names :公司-name))
      (is (contains? names :stock-code))))
  (testing "preserves accented Latin characters (with lowercasing)"
    (let [ds (ds/->dataset {"Société Générale" [1]
                            "Café @ Home" [2]})
          result (du/clean-column-names ds)
          names (set (ds/column-names result))]
      (is (contains? names :société-générale))
      (is (contains? names :café-home))))
  (testing "mixed-script column names work correctly"
    (let [ds (ds/->dataset {"股票代码/Code" ["X"]
                            "价格 ($USD)" [100.0]})
          result (du/clean-column-names ds)
          names (set (ds/column-names result))]
      (is (contains? names :股票代码-code))
      (is (contains? names :价格-usd)))))

(deftest duplicate-rows-all-columns
  (let [ds (ds/->dataset {:id [1 2 2 3] :val ["a" "b" "b" "c"]})
        result (du/duplicate-rows ds)]
    (is (= 2 (ds/row-count result)))
    (is (= [2 2] (vec (:id result))))))

(deftest duplicate-rows-column-subset
  (let [ds (ds/->dataset {:id [1 2 2 3 3] :val ["a" "b" "c" "d" "e"]})
        result (du/duplicate-rows ds [:id])]
    (is (= 4 (ds/row-count result)))
    (is (= [2 2 3 3] (vec (:id result))))))

(deftest duplicate-rows-no-duplicates
  (let [ds (ds/->dataset {:id [1 2 3] :val ["a" "b" "c"]})
        result (du/duplicate-rows ds)]
    (is (= 0 (ds/row-count result)))))

(deftest mark-duplicates-basic
  (let [ds (ds/->dataset {:id [1 2 2 3] :val ["a" "b" "b" "c"]})
        result (du/mark-duplicates ds)]
    (is (contains? (set (ds/column-names result)) :duplicate?))
    (is (= [false true true false] (vec (:duplicate? result))))))

(deftest mark-duplicates-column-subset
  (let [ds (ds/->dataset {:id [1 2 2 3] :val ["a" "b" "c" "d"]})
        result (du/mark-duplicates ds [:id])]
    (is (= [false true true false] (vec (:duplicate? result))))))

(deftest drop-constant-columns-basic
  (let [ds (ds/->dataset {:a [1 1 1] :b [1 2 3] :c ["x" "x" "x"]})
        result (du/drop-constant-columns ds)]
    (is (= [:b] (vec (ds/column-names result))))
    (is (= [1 2 3] (vec (:b result))))))

(deftest drop-constant-columns-all-varying
  (let [ds (ds/->dataset {:a [1 2] :b [3 4]})
        result (du/drop-constant-columns ds)]
    (is (= #{:a :b} (set (ds/column-names result))))))

(deftest drop-constant-columns-one-row
  (let [ds (ds/->dataset {:a [42] :b ["x"]})
        result (du/drop-constant-columns ds)]
    (is (= #{:a :b} (set (ds/column-names result))))))

(deftest coerce-columns-basic
  (let [ds (ds/->dataset {:year [2020 2021 2022] :mass [3750 4200 3900]})
        result (du/coerce-columns ds {:year :float64 :mass :float64})]
    (is (= :float64 (dtype/elemwise-datatype (result :year))))
    (is (= :float64 (dtype/elemwise-datatype (result :mass))))
    (is (= 2020.0 (nth (result :year) 0)))))

(deftest coerce-columns-partial
  (let [ds (ds/->dataset {:a [1 2 3] :b [4 5 6]})
        result (du/coerce-columns ds {:a :float64})]
    (is (= :float64 (dtype/elemwise-datatype (result :a))))
    (is (= :int64 (dtype/elemwise-datatype (result :b))))))
