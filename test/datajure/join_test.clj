(ns datajure.join-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.join :refer [join]]
            [datajure.core :as core]))

(def lhs (ds/->dataset {:id [1 2 3] :x ["a" "b" "c"]}))
(def rhs (ds/->dataset {:id [2 3 4] :y [20 30 40]}))

(deftest inner-join-test
  (let [result (join lhs rhs :on :id :how :inner)]
    (is (= 2 (ds/row-count result)))
    (is (= [2 3] (vec (result :id))))
    (is (= ["b" "c"] (vec (result :x))))
    (is (= [20 30] (vec (result :y))))))

(deftest inner-join-default-test
  (testing ":inner is the default :how"
    (let [result (join lhs rhs :on :id)]
      (is (= 2 (ds/row-count result))))))

(deftest left-join-test
  (let [result (join lhs rhs :on :id :how :left)]
    (is (= 3 (ds/row-count result)))
    (is (= #{1 2 3} (set (result :id))))))

(deftest right-join-test
  (let [result (join lhs rhs :on :id :how :right)]
    (is (= 3 (ds/row-count result)))
    (is (= #{2 3 4} (set (result :id))))))

(deftest outer-join-test
  (let [result (join lhs rhs :on :id :how :outer)]
    (is (= 4 (ds/row-count result)))
    (is (= #{1 2 3 4} (set (result :id))))))

(deftest multi-column-on-test
  (let [l (ds/->dataset {:a [1 1 2] :b ["x" "y" "x"] :val [10 20 30]})
        r (ds/->dataset {:a [1 2 2] :b ["x" "x" "y"] :score [100 200 300]})
        result (join l r :on [:a :b] :how :inner)]
    (is (= 2 (ds/row-count result)))
    (is (= [1 2] (vec (result :a))))
    (is (= ["x" "x"] (vec (result :b))))))

(deftest left-on-right-on-test
  (let [l (ds/->dataset {:id [1 2 3] :x ["a" "b" "c"]})
        r (ds/->dataset {:key [2 3 4] :y [20 30 40]})
        result (join l r :left-on :id :right-on :key :how :left)]
    (is (= 3 (ds/row-count result)))
    (is (= #{1 2 3} (set (result :id))))))

(deftest error-on-with-left-on-test
  (is (thrown-with-msg? Exception #"Cannot combine :on with :left-on"
                        (join lhs rhs :on :id :left-on :id))))

(deftest error-missing-keys-test
  (is (thrown-with-msg? Exception #"Must provide either :on"
                        (join lhs rhs :how :inner))))

(deftest error-unknown-how-test
  (is (thrown-with-msg? Exception #"Unknown join type"
                        (join lhs rhs :on :id :how :cross))))

(deftest composable-with-dt-test
  (let [result (-> (join lhs rhs :on :id :how :left)
                   (core/dt :where #dt/e (> :id 1))
                   (core/dt :select [:id :x :y]))]
    (is (= 2 (ds/row-count result)))
    (is (= [2 3] (vec (result :id))))))

(deftest validate-1-1-pass-test
  (testing ":1:1 passes when both sides have unique keys"
    (let [result (join lhs rhs :on :id :validate :1:1)]
      (is (= 2 (ds/row-count result))))))

(deftest validate-1-1-fail-left-test
  (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})]
    (is (thrown-with-msg? Exception #"left dataset has duplicate keys"
                          (join duped-l rhs :on :id :validate :1:1)))))

(deftest validate-1-1-fail-right-test
  (let [duped-r (ds/->dataset {:id [2 2 3] :y [20 30 40]})]
    (is (thrown-with-msg? Exception #"right dataset has duplicate keys"
                          (join lhs duped-r :on :id :validate :1:1)))))

(deftest validate-1-m-pass-test
  (testing ":1:m passes when left is unique"
    (let [duped-r (ds/->dataset {:id [2 2 3] :y [20 30 40]})
          result (join lhs duped-r :on :id :how :inner :validate :1:m)]
      (is (= 3 (ds/row-count result))))))

(deftest validate-1-m-fail-test
  (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})]
    (is (thrown-with-msg? Exception #"left dataset has duplicate keys"
                          (join duped-l rhs :on :id :validate :1:m)))))

(deftest validate-m-1-pass-test
  (testing ":m:1 passes when right is unique"
    (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})
          result (join duped-l rhs :on :id :how :inner :validate :m:1)]
      (is (= 1 (ds/row-count result))))))

(deftest validate-m-1-fail-test
  (let [duped-r (ds/->dataset {:id [2 2 3] :y [20 30 40]})]
    (is (thrown-with-msg? Exception #"right dataset has duplicate keys"
                          (join lhs duped-r :on :id :validate :m:1)))))

(deftest validate-m-m-pass-test
  (testing ":m:m always passes"
    (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})
          duped-r (ds/->dataset {:id [2 2 3] :y [20 30 40]})
          result (join duped-l duped-r :on :id :how :inner :validate :m:m)]
      (is (= 2 (ds/row-count result))))))

(deftest validate-unknown-value-test
  (is (thrown-with-msg? Exception #"Unknown :validate value"
                        (join lhs rhs :on :id :validate :foo))))

(deftest validate-with-left-on-right-on-test
  (testing ":validate works with asymmetric key names"
    (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})
          r (ds/->dataset {:key [2 3 4] :y [20 30 40]})]
      (is (thrown-with-msg? Exception #"left dataset has duplicate keys"
                            (join duped-l r :left-on :id :right-on :key :validate :1:1))))))

(deftest report-basic-test
  (testing ":report prints merge diagnostics"
    (let [output (with-out-str (join lhs rhs :on :id :how :left :report true))]
      (is (re-find #"2 matched" output))
      (is (re-find #"1 left-only" output))
      (is (re-find #"1 right-only" output)))))

(deftest report-full-overlap-test
  (testing ":report with full overlap shows 0 left-only and right-only"
    (let [r (ds/->dataset {:id [1 2 3] :y [10 20 30]})
          output (with-out-str (join lhs r :on :id :report true))]
      (is (re-find #"3 matched" output))
      (is (re-find #"0 left-only" output))
      (is (re-find #"0 right-only" output)))))

(deftest report-no-output-by-default-test
  (testing "no report output when :report is not set"
    (let [output (with-out-str (join lhs rhs :on :id))]
      (is (= "" output)))))

(deftest multi-column-left-join-test
  (let [l (ds/->dataset {:a [1 1 2] :b ["x" "y" "x"] :val [10 20 30]})
        r (ds/->dataset {:a [1 2 2] :b ["x" "x" "y"] :score [100 200 300]})
        result (join l r :on [:a :b] :how :left)]
    (is (= 3 (ds/row-count result)))
    (is (= #{[1 "x"] [1 "y"] [2 "x"]} (set (map vector (result :a) (result :b)))))))

(deftest no-overlap-join-test
  (let [l (ds/->dataset {:id [1 2] :x ["a" "b"]})
        r (ds/->dataset {:id [3 4] :y [30 40]})]
    (testing "inner join with no overlap returns empty"
      (is (= 0 (ds/row-count (join l r :on :id :how :inner)))))
    (testing "left join preserves all left rows"
      (is (= 2 (ds/row-count (join l r :on :id :how :left)))))
    (testing "report shows 0 matched"
      (let [output (with-out-str (join l r :on :id :report true))]
        (is (re-find #"0 matched" output))
        (is (re-find #"2 left-only" output))
        (is (re-find #"2 right-only" output))))))

(deftest validate-multi-column-on-test
  (testing ":validate with multi-column keys"
    (let [l (ds/->dataset {:a [1 1 2] :b ["x" "x" "y"] :val [10 20 30]})
          r (ds/->dataset {:a [1 2] :b ["x" "y"] :score [100 200]})]
      (is (thrown-with-msg? Exception #"left dataset has duplicate keys"
                            (join l r :on [:a :b] :validate :1:1))))))

(deftest validate-and-report-together-test
  (testing ":validate and :report work together"
    (let [output (with-out-str (join lhs rhs :on :id :how :left :validate :1:1 :report true))]
      (is (re-find #"2 matched" output)))))

(deftest report-with-left-on-right-on-test
  (testing ":report works with asymmetric key names"
    (let [r (ds/->dataset {:key [2 3 4] :y [20 30 40]})
          output (with-out-str (join lhs r :left-on :id :right-on :key :how :left :report true))]
      (is (re-find #"2 matched" output))
      (is (re-find #"1 left-only" output)))))

(deftest ex-data-structure-test
  (testing "cardinality error has structured ex-data"
    (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})]
      (try (join duped-l rhs :on :id :validate :1:1)
           (catch Exception e
             (let [d (ex-data e)]
               (is (= :join-cardinality-violation (:dt/error d)))
               (is (= :1:1 (:dt/validate d)))
               (is (= :left (:dt/side d)))
               (is (= [:id] (:dt/keys d)))))))))

(deftest full-pipeline-with-join-test
  (testing "join → dt pipeline matching spec examples"
    (let [result (-> (join lhs rhs :on :id :how :left)
                     (core/dt :where #dt/e (> :y 0)
                              :set {:y2 #dt/e (* :y 2)})
                     (core/dt :order-by [(core/desc :y2)])
                     (core/dt :select [:id :y2]))]
      (is (= 2 (ds/row-count result)))
      (is (= [60 40] (vec (result :y2)))))))
