(ns datajure.math-test
  (:require [clojure.test :refer [deftest is testing]]
            [datajure.math :as math]
            [tech.v3.dataset :as ds]))

(deftest quantile-type7-matches-r
  ;; Reference values from R's quantile(x, p, type = 7) — the cases recorded in
  ;; the investing-app DATAJURE-NOTES (where dfn/percentiles disagreed).
  (testing "1..11 at p20/p50/p80"
    (is (== 3.0 (math/quantile-type7 (range 1 12) 0.2)))
    (is (== 6.0 (math/quantile-type7 (range 1 12) 0.5)))
    (is (== 9.0 (math/quantile-type7 (range 1 12) 0.8))))
  (testing "1..10 (interpolated) at p20/p50/p80"
    (is (== 2.8 (math/quantile-type7 (range 1 11) 0.2)))
    (is (== 5.5 (math/quantile-type7 (range 1 11) 0.5)))
    (is (== 8.2 (math/quantile-type7 (range 1 11) 0.8))))
  (testing "p clamps at the endpoints"
    (is (== 1.0 (math/quantile-type7 (range 1 12) 0.0)))
    (is (== 11.0 (math/quantile-type7 (range 1 12) 1.0)))))

(deftest quantile-type7-drops-non-finite
  (testing "nil is dropped (R na.rm)"
    (is (== 2.0 (math/quantile-type7 [1 2 nil 3 nil] 0.5))))
  (testing "±Inf and NaN are dropped (R is.finite)"
    (is (== 2.0 (math/quantile-type7 [1.0 2.0 ##Inf ##-Inf 3.0] 0.5)))
    (is (== 2.0 (math/quantile-type7 [1.0 ##NaN 2.0 3.0] 0.5))))
  (testing "a filtered lazy seq is accepted (not just a reader)"
    (is (== 2.0 (math/quantile-type7 (remove nil? [1 2 nil 3]) 0.5))))
  (testing "a dataset column is accepted"
    (is (== 2.0 (math/quantile-type7 ((ds/->dataset {:x [1.0 2.0 3.0]}) :x) 0.5)))))

(deftest quantiles-type7-test
  (testing "multi-probability matches the single-probability calls, sorting once"
    (is (= (mapv #(math/quantile-type7 (range 1 12) %) [0.2 0.5 0.8])
           (math/quantiles-type7 (range 1 12) [0.2 0.5 0.8])))
    (is (= [3.0 6.0 9.0] (math/quantiles-type7 (range 1 12) [0.2 0.5 0.8]))))
  (testing "min-n applies per element"
    (is (= [nil nil nil] (math/quantiles-type7 [1 2 3] [0.2 0.5 0.8] 11))))
  (testing "drops nil + non-finite like the scalar form"
    (is (= [2.0] (math/quantiles-type7 [1.0 2.0 ##Inf nil 3.0] [0.5])))))

(deftest quantile-non-numeric-guard
  ;; A date/temporal (or otherwise non-numeric) column can't be ranked — throw a
  ;; structured error, not a raw ClassCastException deep in the double-cast.
  (let [err (fn [thunk] (-> (try (thunk) nil (catch clojure.lang.ExceptionInfo e e))
                            ex-data :dt/error))]
    (is (= :quantile-non-numeric
           (err #(math/quantile-type7 [(java.time.LocalDate/of 2020 1 1)] 0.5))))
    (is (= :quantile-non-numeric
           (err #(math/quantiles-type7 ["x" "y"] [0.5]))))))

(deftest finite-double?-test
  (is (true? (math/finite-double? 0)))
  (is (true? (math/finite-double? -3.5)))
  (is (false? (boolean (math/finite-double? nil))))
  (is (false? (math/finite-double? ##NaN)))
  (is (false? (math/finite-double? ##Inf)))
  (is (false? (math/finite-double? ##-Inf))))

(deftest asinh-test
  (testing "matches sign(x)·ln(|x|+sqrt(x²+1))"
    (is (== 0.0 (math/asinh 0.0)))
    (is (< (Math/abs (- (math/asinh 1.0) 0.881373587019543)) 1e-12))
    ;; odd symmetry
    (is (== (math/asinh 5.0) (- (math/asinh -5.0)))))
  (testing "stable for large negative x (textbook form would collapse to nil/-Inf)"
    (is (< (math/asinh -1.0e6) -14.0))
    (is (math/finite-double? (math/asinh -1.0e8))))
  (testing "nil / non-finite -> nil"
    (is (nil? (math/asinh nil)))
    (is (nil? (math/asinh ##NaN)))
    (is (nil? (math/asinh ##Inf)))))

(deftest quantile-type7-edge-cases
  (testing "no finite values -> nil"
    (is (nil? (math/quantile-type7 [nil nil] 0.5)))
    (is (nil? (math/quantile-type7 [##Inf ##NaN] 0.5)))
    (is (nil? (math/quantile-type7 [] 0.5))))
  (testing "single value -> that value regardless of p"
    (is (== 42.0 (math/quantile-type7 [42.0] 0.2)))
    (is (== 42.0 (math/quantile-type7 [42.0] 0.9))))
  (testing "min-n floor: returns nil below the threshold, value at/above"
    (is (nil? (math/quantile-type7 (range 1 11) 0.2 11)))  ;; 10 finite < 11
    (is (== 3.0 (math/quantile-type7 (range 1 12) 0.2 11))) ;; 11 finite
    (is (nil? (math/quantile-type7 [1 2 nil 3] 0.5 4)))     ;; 3 finite < 4 -> nil
    (is (== 2.0 (math/quantile-type7 [1 2 3] 0.5 3)))))     ;; exactly 3 finite >= 3 -> ok
