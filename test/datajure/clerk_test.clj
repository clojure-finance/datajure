(ns datajure.clerk-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.clerk :as dc]
            [datajure.expr :as expr]
            [datajure.util :as du]))

(def ^:private test-ds
  (ds/->dataset {:species ["Adelie" "Gentoo" "Chinstrap"]
                 :mass [3750 5000 3500]
                 :year [2007 2008 2007]
                 :height [1.5 1.8 1.4]}))

(def ^:private ds-with-nil
  (ds/->dataset {:a [1 nil 3]
                 :b [nil nil 6.0]}))

(deftest dataset->hiccup-basic
  (let [h (dc/dataset->hiccup test-ds)]
    (is (= :div (first h)))
    (is (map? (second h)))
    (testing "header shows row x col count"
      (is (re-find #"3 rows" (str h))))
    (testing "contains column names"
      (is (re-find #"species" (str h)))
      (is (re-find #"mass" (str h))))))

(deftest dataset->hiccup-truncation
  (let [big-ds (ds/->dataset {:x (range 50) :y (range 50)})
        h (dc/dataset->hiccup big-ds {:max-rows 5})]
    (testing "truncation message"
      (is (re-find #"45 more rows" (str h))))))

(deftest dataset->hiccup-col-truncation
  (let [wide-ds (ds/->dataset (zipmap (map #(keyword (str "c" %)) (range 10))
                                      (repeat 10 [1 2])))
        h (dc/dataset->hiccup wide-ds {:max-cols 3})]
    (testing "column truncation indicator"
      (is (re-find #"…" (str h))))))

(deftest dataset->hiccup-nil-values
  (let [h (dc/dataset->hiccup ds-with-nil)]
    (testing "nil values rendered"
      (is (re-find #"nil" (str h))))
    (testing "missing count shown"
      (is (re-find #"1 nil" (str h))))))

(deftest dataset->hiccup-type-badges
  (let [h (str (dc/dataset->hiccup test-ds))]
    (testing "type badges present"
      (is (re-find #"int64" h))
      (is (re-find #"string" h))
      (is (re-find #"float64" h)))))

(deftest ast->string-basic-ops
  (is (= "(> :mass 4000)" (dc/ast->string #dt/e (> :mass 4000))))
  (is (= "(/ :mass (sq :height))" (dc/ast->string #dt/e (/ :mass (sq :height)))))
  (is (= "(+ :a :b)" (dc/ast->string #dt/e (+ :a :b))))
  (is (= "(and (> :mass 4000) (< :year 2010))"
         (dc/ast->string #dt/e (and (> :mass 4000) (< :year 2010))))))

(deftest ast->string-window-ops
  (is (= "(win/rank :mass)" (dc/ast->string #dt/e (win/rank :mass))))
  (is (= "(win/lag :mass 1)" (dc/ast->string #dt/e (win/lag :mass 1))))
  (is (= "(win/cumsum :val)" (dc/ast->string #dt/e (win/cumsum :val))))
  (is (= "(win/scan + :x)" (dc/ast->string #dt/e (win/scan + :x))))
  (is (= "(win/scan * (+ 1 :ret))" (dc/ast->string #dt/e (win/scan * (+ 1 :ret))))))

(deftest ast->string-row-ops
  (is (= "(row/sum :a :b :c)" (dc/ast->string #dt/e (row/sum :a :b :c))))
  (is (= "(row/mean :x :y)" (dc/ast->string #dt/e (row/mean :x :y)))))

(deftest ast->string-if-and-cond
  (testing "simple if"
    (is (= "(if (> :x 0) \"pos\" \"neg\")"
           (dc/ast->string #dt/e (if (> :x 0) "pos" "neg")))))
  (testing "cond reconstructed"
    (is (= "(cond (> :bmi 30) \"obese\" :else \"normal\")"
           (dc/ast->string #dt/e (cond (> :bmi 30) "obese" :else "normal"))))
    (is (= "(cond (> :bmi 40) \"severe\" (> :bmi 30) \"obese\" :else \"normal\")"
           (dc/ast->string #dt/e (cond (> :bmi 40) "severe" (> :bmi 30) "obese" :else "normal"))))))

(deftest ast->string-let
  (is (= "(let [x (+ :a :b)] (* x 2))"
         (dc/ast->string #dt/e (let [x (+ :a :b)] (* x 2))))))

(deftest ast->string-coalesce
  (is (= "(coalesce :mass 0)" (dc/ast->string #dt/e (coalesce :mass 0)))))

(deftest ast->string-membership
  (is (re-find #"in :species" (dc/ast->string #dt/e (in :species #{"Gentoo"}))))
  (is (= "(between? :year 2005 2010)" (dc/ast->string #dt/e (between? :year 2005 2010)))))

(deftest ast->string-xbar
  (is (= "(xbar :price 10)" (dc/ast->string #dt/e (xbar :price 10))))
  (is (= "(xbar :t 5 :minutes)" (dc/ast->string #dt/e (xbar :t 5 :minutes)))))

(deftest ast->string-composition
  (let [bmi #dt/e (/ :mass (sq :height))]
    (is (= "(mean (/ :mass (sq :height)))" (dc/ast->string #dt/e (mn bmi))))))

(deftest expr->hiccup-basic
  (let [h (dc/expr->hiccup #dt/e (> :mass 4000))]
    (is (= :div (first h)))
    (testing "shows expression text"
      (is (re-find #"mass" (str h))))
    (testing "shows column refs"
      (is (re-find #"mass" (str h))))))

(deftest expr->hiccup-window
  (let [h (dc/expr->hiccup #dt/e (win/rank :mass))]
    (testing "window badge shown"
      (is (re-find #"window" (str h))))))

(deftest describe->hiccup-basic
  (let [desc (du/describe test-ds)
        h (dc/describe->hiccup desc)]
    (is (= :div (first h)))
    (testing "header shows describe"
      (is (re-find #"describe" (str h))))
    (testing "column names in output"
      (is (re-find #"species" (str h)))
      (is (re-find #"mass" (str h))))))

(deftest describe->hiccup-missing-highlight
  (let [desc (du/describe ds-with-nil)
        h-str (str (dc/describe->hiccup desc))]
    (testing "missing counts present"
      (is (re-find #"ef5350" h-str)))))

(deftest viewer-preds
  (testing "dataset-viewer pred"
    (is ((:pred dc/dataset-viewer) test-ds))
    (is (not ((:pred dc/dataset-viewer) {:a 1})))
    (is (not ((:pred dc/dataset-viewer) 42))))
  (testing "expr-viewer pred"
    (is ((:pred dc/expr-viewer) #dt/e (> :mass 4000)))
    (is (not ((:pred dc/expr-viewer) {:a 1}))))
  (testing "describe-viewer pred"
    (is ((:pred dc/describe-viewer) (du/describe test-ds)))
    (is (not ((:pred dc/describe-viewer) test-ds)))))
