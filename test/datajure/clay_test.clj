(ns datajure.clay-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.clay :as dc]
            [datajure.expr :as expr]
            [datajure.util :as du]))

(def test-ds
  (ds/->dataset {:species ["Adelie" "Gentoo" "Chinstrap"]
                 :year [2007 2008 2009]
                 :mass [3750.0 5200.0 3800.0]}))

(def ds-with-nil
  (ds/->dataset {:x [1.0 nil 3.0]
                 :y ["a" "b" nil]}))

(deftest view-basic
  (let [h (dc/view test-ds)]
    (testing "returns Hiccup vector"
      (is (vector? h))
      (is (= :div (first h))))
    (testing "has Kindly :kind/hiccup metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))
    (testing "contains dataset dimensions"
      (let [s (str h)]
        (is (re-find #"3 rows" s))
        (is (re-find #"3 cols" s))))))

(deftest view-with-opts
  (let [big-ds (ds/->dataset {:x (range 50)})
        h (dc/view big-ds {:max-rows 5})]
    (testing "has Kindly metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))
    (testing "truncates rows"
      (is (re-find #"more rows" (str h))))))

(deftest view-nil-values
  (let [h (dc/view ds-with-nil)]
    (testing "renders without error"
      (is (vector? h)))
    (testing "has Kindly metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))
    (testing "shows nil indicator"
      (is (re-find #"nil" (str h))))))

(deftest view-expr-basic
  (let [node #dt/e (> :mass 4000)
        h (dc/view-expr node)]
    (testing "returns Hiccup vector"
      (is (vector? h))
      (is (= :div (first h))))
    (testing "has Kindly :kind/hiccup metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))
    (testing "contains expression text"
      (is (re-find #":mass" (str h))))
    (testing "contains column refs"
      (is (re-find #"mass" (str h))))))

(deftest view-expr-window
  (let [node #dt/e (win/rank :mass)
        h (dc/view-expr node)]
    (testing "has Kindly metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))
    (testing "shows window badge"
      (is (re-find #"window" (str h)))))
  (let [node #dt/e (win/scan * (+ 1 :ret))
        h (dc/view-expr node)]
    (testing "win/scan has Kindly metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))
    (testing "win/scan shows window badge"
      (is (re-find #"window" (str h))))))

(deftest view-expr-composition
  (let [bmi #dt/e (/ :mass (sq :height))
        node #dt/e (> bmi 30)
        h (dc/view-expr node)]
    (testing "renders composed expression"
      (is (vector? h)))
    (testing "has Kindly metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))))

(deftest view-describe-basic
  (let [desc (du/describe test-ds)
        h (dc/view-describe desc)]
    (testing "returns Hiccup vector"
      (is (vector? h))
      (is (= :div (first h))))
    (testing "has Kindly :kind/hiccup metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))
    (testing "contains describe header"
      (is (re-find #"describe" (str h))))))

(deftest view-describe-missing
  (let [desc (du/describe ds-with-nil)
        h (dc/view-describe desc)]
    (testing "has Kindly metadata"
      (is (= :kind/hiccup (:kindly/kind (meta h)))))
    (testing "highlights missing data"
      (is (re-find #"ef5350" (str h))))))

(deftest datajure-advisor-dataset
  (let [result (dc/datajure-advisor {:value test-ds})]
    (testing "recognizes dataset"
      (is (= [[:kind/hiccup]] result)))))

(deftest datajure-advisor-expr
  (let [result (dc/datajure-advisor {:value #dt/e (> :mass 4000)})]
    (testing "recognizes #dt/e expression"
      (is (= [[:kind/hiccup]] result)))))

(deftest datajure-advisor-describe
  (let [desc (du/describe test-ds)
        result (dc/datajure-advisor {:value desc})]
    (testing "recognizes describe output"
      (is (= [[:kind/hiccup]] result)))))

(deftest datajure-advisor-other
  (let [result (dc/datajure-advisor {:value 42})]
    (testing "returns nil for non-datajure values"
      (is (nil? result)))))

(deftest datajure-advisor-describe-before-dataset
  (testing "describe output is recognized as describe, not generic dataset"
    (let [desc (du/describe test-ds)
          result-desc (dc/datajure-advisor {:value desc})
          result-ds (dc/datajure-advisor {:value test-ds})]
      (is (= result-desc result-ds))
      (is (= [[:kind/hiccup]] result-desc)))))
