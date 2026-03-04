(ns datajure.concise-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.concise :refer [mn sm md sd ct nuniq N dt asc desc rename pass-nil
                                      fst lst wa ws mx mi]]))

(def sample
  (ds/->dataset {:species ["Adelie" "Adelie" "Gentoo"]
                 :mass [3750.0 3800.0 5000.0]
                 :year [2007 2008 2007]}))

(deftest concise-agg-helpers
  (testing "mn/sm/md/sd operate on column vectors"
    (let [col (:mass sample)]
      (is (= (double (mn col)) (/ (+ 3750.0 3800.0 5000.0) 3)))
      (is (= (double (sm col)) 12550.0))
      (is (number? (md col)))
      (is (number? (sd col)))))
  (testing "ct returns element count"
    (is (= 3 (ct (:mass sample)))))
  (testing "nuniq returns distinct count"
    (is (= 2 (nuniq (:species sample))))))

(deftest concise-core-re-exports
  (testing "N works as row count agg"
    (let [result (dt sample :by [:species] :agg {:n N})]
      (is (= 2 (ds/row-count result)))))
  (testing "asc/desc produce sort specs"
    (is (= {:order :asc :col :mass} (asc :mass)))
    (is (= {:order :desc :col :mass} (desc :mass))))
  (testing "rename re-exported correctly"
    (let [result (rename sample {:mass :weight-kg})]
      (is (contains? (set (ds/column-names result)) :weight-kg))
      (is (not (contains? (set (ds/column-names result)) :mass))))))

(deftest concise-fst-lst
  (testing "fst returns the first value, lst returns the last"
    (let [col (:mass sample)]
      (is (= 3750.0 (fst col)))
      (is (= 5000.0 (lst col))))))

(deftest concise-wa-ws
  (testing "wa computes weighted average"
    (let [w [1.0 2.0 3.0]
          v [10.0 20.0 30.0]]
      (is (< (Math/abs (- (wa w v) (/ (+ 10.0 40.0 90.0) 6.0))) 1e-9))))
  (testing "ws computes weighted sum"
    (let [w [1.0 2.0 3.0]
          v [10.0 20.0 30.0]]
      (is (= 140.0 (ws w v)))))
  (testing "wa skips nil pairs"
    (let [w [1.0 nil 2.0]
          v [10.0 20.0 nil]]
      (is (= 10.0 (wa w v))))))

(deftest concise-mx-mi
  (testing "mx returns column maximum"
    (let [col (:mass sample)]
      (is (= 5000.0 (mx col)))))
  (testing "mi returns column minimum"
    (let [col (:mass sample)]
      (is (= 3750.0 (mi col))))))
