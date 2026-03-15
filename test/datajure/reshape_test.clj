(ns datajure.reshape-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.core :as core]
            [datajure.reshape :refer [melt]]))

(def ^:private wide-ds
  (ds/->dataset {:species ["Adelie" "Gentoo"]
                 :year [2007 2008]
                 :mass [3750 5000]
                 :flipper [181 210]}))

(deftest melt-basic
  (testing "melt with explicit id and measure cols"
    (let [result (melt wide-ds {:id [:species :year] :measure [:mass :flipper]})]
      (is (= 4 (ds/row-count result)))
      (is (= #{:species :year :variable :value} (set (ds/column-names result)))))))

(deftest melt-infers-measure-cols
  (testing "melt infers measure as all non-id columns"
    (let [result (melt wide-ds {:id [:species :year]})]
      (is (= 4 (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :variable))
      (is (contains? (set (ds/column-names result)) :value)))))

(deftest melt-variable-col-contains-col-names
  (testing ":variable column contains original measure column names as strings"
    (let [result (melt wide-ds {:id [:species :year] :measure [:mass :flipper]})]
      (is (= #{"mass" "flipper"} (set (ds/column result :variable)))))))

(deftest melt-value-col-contains-values
  (testing ":value column contains original measure values"
    (let [result (melt wide-ds {:id [:species :year] :measure [:mass]})]
      (is (= [3750 5000] (vec (ds/column result :value)))))))

(deftest melt-custom-col-names
  (testing "custom :variable-col and :value-col names"
    (let [result (melt wide-ds {:id [:species :year] :measure [:mass :flipper]
                                :variable-col :metric :value-col :val})]
      (is (= #{:species :year :metric :val} (set (ds/column-names result))))
      (is (= #{"mass" "flipper"} (set (ds/column result :metric)))))))

(deftest melt-preserves-id-values
  (testing "id columns are preserved correctly across all rows"
    (let [result (melt wide-ds {:id [:species] :measure [:mass :flipper]})]
      (is (= 4 (ds/row-count result)))
      (is (= #{"Adelie" "Gentoo"} (set (ds/column result :species)))))))

(deftest melt-row-count
  (testing "row count = nrow * n-measure-cols"
    (let [ds3 (ds/->dataset {:id [1 2 3] :a [10 20 30] :b [40 50 60] :c [70 80 90]})
          result (melt ds3 {:id [:id] :measure [:a :b :c]})]
      (is (= 9 (ds/row-count result))))))

(deftest melt-threads-with-dt
  (testing "melt result threads naturally with datajure.core/dt"
    (let [result (-> (melt wide-ds {:id [:species :year] :measure [:mass :flipper]})
                     (core/dt :where #dt/e (= :variable "mass")))]
      (is (= 2 (ds/row-count result))))))

(deftest melt-empty-measure-cols
  (testing "melt with all id columns returns empty dataset with correct schema"
    (let [ds (ds/->dataset {:a [1 2 3] :b [4 5 6]})
          result (melt ds {:id [:a :b]})]
      (is (ds/dataset? result))
      (is (= 0 (ds/row-count result)))
      (is (= #{:a :b :variable :value} (set (ds/column-names result))))))
  (testing "melt with explicit empty :measure returns empty dataset"
    (let [ds (ds/->dataset {:a [1 2] :b [3 4]})
          result (melt ds {:id [:a] :measure []})]
      (is (ds/dataset? result))
      (is (= 0 (ds/row-count result)))
      (is (= #{:a :variable :value} (set (ds/column-names result))))))
  (testing "melt with custom col names on empty measure"
    (let [ds (ds/->dataset {:a [1] :b [2]})
          result (melt ds {:id [:a :b] :variable-col :metric :value-col :val})]
      (is (ds/dataset? result))
      (is (= 0 (ds/row-count result)))
      (is (= #{:a :b :metric :val} (set (ds/column-names result)))))))
