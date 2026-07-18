(ns datajure.stat-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [datajure.expr :as expr]
            [datajure.stat :as stat]
            [datajure.core :as core]
            [datajure.concise :as c]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn- approx= [a b tol]
  (< (Math/abs (- (double a) (double b))) tol))

(defn- col->vec [col]
  (into [] col))

;; ---------------------------------------------------------------------------
;; AST parsing
;; ---------------------------------------------------------------------------

(deftest stat-ast-parsing-standardize
  (let [node (expr/read-expr '(stat/standardize :x))]
    (testing "node type is :stat"
      (is (= :stat (:node/type node))))
    (testing "op is :stat/standardize"
      (is (= :stat/standardize (:stat/op node))))
    (testing "args parsed"
      (is (= 1 (count (:stat/args node))))
      (is (= :col (-> node :stat/args first :node/type)))
      (is (= :x (-> node :stat/args first :col/name))))))

(deftest stat-ast-parsing-demean
  (let [node (expr/read-expr '(stat/demean :ret))]
    (is (= :stat (:node/type node)))
    (is (= :stat/demean (:stat/op node)))
    (is (= :ret (-> node :stat/args first :col/name)))))

(deftest stat-ast-parsing-winsorize
  (let [node (expr/read-expr '(stat/winsorize :price 0.01))]
    (is (= :stat (:node/type node)))
    (is (= :stat/winsorize (:stat/op node)))
    (is (= 2 (count (:stat/args node))))
    (is (= :price (-> node :stat/args first :col/name)))
    (is (= 0.01 (-> node :stat/args second :lit/value)))))

;; ---------------------------------------------------------------------------
;; col-refs traversal
;; ---------------------------------------------------------------------------

(deftest stat-col-refs
  (testing "standardize extracts column ref"
    (is (= #{:x} (expr/col-refs (expr/read-expr '(stat/standardize :x))))))
  (testing "demean extracts column ref"
    (is (= #{:ret} (expr/col-refs (expr/read-expr '(stat/demean :ret))))))
  (testing "winsorize extracts column ref from first arg only"
    (is (= #{:price} (expr/col-refs (expr/read-expr '(stat/winsorize :price 0.01))))))
  (testing "stat inside arithmetic extracts refs"
    (is (= #{:x} (expr/col-refs (expr/read-expr '(* 2 (stat/demean :x))))))))

;; ---------------------------------------------------------------------------
;; win-refs traversal — stat ops contain no window refs
;; ---------------------------------------------------------------------------

(deftest stat-win-refs
  (is (= #{} (expr/win-refs (expr/read-expr '(stat/standardize :x)))))
  (is (= #{} (expr/win-refs (expr/read-expr '(stat/winsorize :ret 0.05))))))

;; ---------------------------------------------------------------------------
;; stat/stat-standardize runtime
;; ---------------------------------------------------------------------------

(deftest standardize-basic
  (let [ds (ds/->dataset {:x [1.0 2.0 3.0 4.0 5.0]})
        result (col->vec (stat/stat-standardize (ds :x)))]
    (testing "mean of standardized column is ~0"
      (is (approx= (/ (reduce + result) (count result)) 0.0 1e-10)))
    (testing "sample sd of standardized column is ~1"
      (is (approx= (dfn/standard-deviation result) 1.0 1e-10)))
    (testing "middle element is 0"
      (is (approx= (nth result 2) 0.0 1e-10)))))

(deftest standardize-nil-preserved
  (let [ds (ds/->dataset {:x [1.0 nil 3.0]})
        result (col->vec (stat/stat-standardize (ds :x)))]
    (is (some? (nth result 0)))
    (is (nil? (nth result 1)))
    (is (some? (nth result 2)))))

(deftest standardize-all-nil
  (let [ds (ds/->dataset {:x [nil nil nil]})
        result (col->vec (stat/stat-standardize (ds :x)))]
    (is (every? nil? result))))

(deftest standardize-zero-variance
  (let [ds (ds/->dataset {:x [5.0 5.0 5.0]})
        result (col->vec (stat/stat-standardize (ds :x)))]
    (is (every? nil? result))))

;; ---------------------------------------------------------------------------
;; stat/stat-demean runtime
;; ---------------------------------------------------------------------------

(deftest demean-basic
  (let [ds (ds/->dataset {:x [1.0 2.0 3.0 4.0 5.0]})
        result (col->vec (stat/stat-demean (ds :x)))]
    (testing "mean is removed: sum is ~0"
      (is (approx= (reduce + result) 0.0 1e-10)))
    (testing "values are x - 3 (mean=3)"
      (is (approx= (nth result 0) -2.0 1e-10))
      (is (approx= (nth result 2) 0.0 1e-10))
      (is (approx= (nth result 4) 2.0 1e-10)))))

(deftest demean-nil-preserved
  (let [ds (ds/->dataset {:x [10.0 nil 30.0]})
        result (col->vec (stat/stat-demean (ds :x)))]
    (is (nil? (nth result 1)))
    (is (some? (nth result 0)))
    (is (some? (nth result 2)))))

(deftest demean-all-nil
  (let [ds (ds/->dataset {:x [nil nil]})
        result (col->vec (stat/stat-demean (ds :x)))]
    (is (every? nil? result))))

;; ---------------------------------------------------------------------------
;; stat/stat-winsorize runtime
;; ---------------------------------------------------------------------------

(deftest winsorize-basic
  (let [ds (ds/->dataset {:x [1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0 10.0]})
        result (col->vec (stat/stat-winsorize (ds :x) 0.1))]
    (testing "count preserved"
      (is (= 10 (count result))))
    (testing "min is clipped up"
      (is (>= (apply min (filter some? result))
              (apply min (filter some? (col->vec (ds :x)))))))
    (testing "max is clipped down"
      (is (<= (apply max (filter some? result))
              (apply max (filter some? (col->vec (ds :x)))))))))

(deftest winsorize-extreme-clipped
  (let [ds (ds/->dataset {:x [0.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 1.0 100.0]})
        result (col->vec (stat/stat-winsorize (ds :x) 0.1))
        lo (first result)
        hi (last result)]
    (testing "bottom outlier clipped up"
      (is (> lo 0.0)))
    (testing "top outlier clipped down"
      (is (< hi 100.0)))))

(deftest winsorize-nil-preserved
  (let [ds (ds/->dataset {:x [1.0 nil 10.0 nil 5.0]})
        result (col->vec (stat/stat-winsorize (ds :x) 0.1))]
    (is (nil? (nth result 1)))
    (is (nil? (nth result 3)))
    (is (some? (nth result 0)))))

(deftest winsorize-all-nil
  (let [ds (ds/->dataset {:x [nil nil nil]})
        result (col->vec (stat/stat-winsorize (ds :x) 0.05))]
    (is (every? nil? result))))

;; ---------------------------------------------------------------------------
;; #dt/e end-to-end through dt
;; ---------------------------------------------------------------------------

(deftest dt-stat-standardize
  (let [ds (ds/->dataset {:x [1.0 2.0 3.0 4.0 5.0]})
        result (core/dt ds :set {:z #dt/e (stat/standardize :x)})]
    (testing "column :z added"
      (is (contains? (set (ds/column-names result)) :z)))
    (testing "original column preserved"
      (is (contains? (set (ds/column-names result)) :x)))
    (testing "middle z-score is 0"
      (is (approx= (nth (col->vec (result :z)) 2) 0.0 1e-10)))))

(deftest dt-stat-demean
  (let [ds (ds/->dataset {:ret [0.1 0.2 0.3]})
        result (core/dt ds :set {:dm #dt/e (stat/demean :ret)})]
    (is (approx= (reduce + (col->vec (result :dm))) 0.0 1e-10))))

(deftest dt-stat-winsorize
  (let [ds (ds/->dataset {:x [1.0 2.0 3.0 4.0 5.0 6.0 7.0 8.0 9.0 10.0]})
        result (core/dt ds :set {:wx #dt/e (stat/winsorize :x 0.1)})]
    (is (= 10 (ds/row-count result)))
    (is (contains? (set (ds/column-names result)) :wx))))

(deftest dt-stat-column-validation
  (testing "unknown column throws with suggestion"
    (let [ds (ds/->dataset {:mass [1.0 2.0]})]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown column"
                            (core/dt ds :set {:z #dt/e (stat/standardize :maas)}))))))

(deftest dt-stat-composed-with-arithmetic
  (let [ds (ds/->dataset {:x [2.0 4.0 6.0]})
        result (core/dt ds :set {:scaled #dt/e (* 2 (stat/demean :x))})]
    (testing "demean then scale"
      (is (approx= (nth (col->vec (result :scaled)) 0) -4.0 1e-10))
      (is (approx= (nth (col->vec (result :scaled)) 1) 0.0 1e-10))
      (is (approx= (nth (col->vec (result :scaled)) 2) 4.0 1e-10)))))

;; ---------------------------------------------------------------------------
;; concise aliases
;; ---------------------------------------------------------------------------

(deftest concise-standardize-alias
  (let [col (-> (ds/->dataset {:x [1.0 2.0 3.0]}) :x)]
    (is (= (col->vec (stat/stat-standardize col))
           (col->vec (c/standardize col))))))

(deftest concise-demean-alias
  (let [col (-> (ds/->dataset {:x [1.0 2.0 3.0]}) :x)]
    (is (= (col->vec (stat/stat-demean col))
           (col->vec (c/demean col))))))

(deftest concise-winsorize-alias
  (let [col (-> (ds/->dataset {:x [1.0 2.0 3.0 4.0 5.0]}) :x)]
    (is (= (col->vec (stat/stat-winsorize col 0.1))
           (col->vec (c/winsorize col 0.1))))))

;; ---------------------------------------------------------------------------
;; Error handling
;; ---------------------------------------------------------------------------

(deftest unknown-stat-op-error
  (let [bad-node {:node/type :stat
                  :stat/op :stat/unknown
                  :stat/args [{:node/type :col :col/name :x}]}]
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown stat op"
                          (expr/compile-expr bad-node)))))

;; ---------------------------------------------------------------------------
;; stat/trim — quantile-based trimming (mbmisc trim)
;; ---------------------------------------------------------------------------

(deftest stat-trim-basic
  (let [col (-> (ds/->dataset {:x [1.0 2.0 3.0 4.0 100.0]}) :x)
        r (col->vec (stat/stat-trim col 0.2))]
    (testing "values outside the [p, 1-p] quantiles become nil; the middle survives"
      (is (nil? (first r)))
      (is (nil? (last r)))
      (is (= [2.0 3.0 4.0] (subvec r 1 4))))))

(deftest stat-trim-nil-handling
  (testing "nil preserved, all-nil column stays all-nil"
    (is (= [nil nil 2.0 3.0 nil nil]
           (col->vec (stat/stat-trim [nil 1.0 2.0 3.0 100.0 nil] 0.25))))
    (is (= [nil nil] (col->vec (stat/stat-trim [nil nil] 0.1))))))

(deftest stat-trim-in-dt
  (let [d (ds/->dataset {:x [1.0 2.0 3.0 4.0 100.0]})
        r (core/dt d :set {:t #dt/e (stat/trim :x 0.2)})]
    (is (= [nil 2.0 3.0 4.0 nil] (col->vec (:t r))))
    (testing "data-form spelling matches"
      (is (= (col->vec (:t r))
             (col->vec (:t (core/dt d :set [[:t [:stat/trim :x 0.2]]]))))))))

;; ---------------------------------------------------------------------------
;; stat/rescale — min-max rescaling (mbmisc mb.rescale / scales::rescale)
;; ---------------------------------------------------------------------------

(deftest stat-rescale-basic
  (testing "default target range [0,1]"
    (is (= [0.0 0.5 1.0] (col->vec (stat/stat-rescale [0.0 5.0 10.0])))))
  (testing "explicit target range"
    (is (= [-1.0 0.0 1.0] (col->vec (stat/stat-rescale [0.0 5.0 10.0] -1.0 1.0)))))
  (testing "nil preserved"
    (is (= [0.0 nil 1.0] (col->vec (stat/stat-rescale [0.0 nil 10.0])))))
  (testing "constant column → all nil (no defined scale)"
    (is (= [nil nil nil] (col->vec (stat/stat-rescale [5.0 5.0 5.0])))))
  (testing "all-nil column stays all-nil"
    (is (= [nil nil] (col->vec (stat/stat-rescale [nil nil]))))))

(deftest stat-rescale-in-dt
  (let [d (ds/->dataset {:x [2.0 4.0 6.0]})]
    (is (= [0.0 0.5 1.0] (col->vec (:r (core/dt d :set {:r #dt/e (stat/rescale :x)})))))
    (is (= [0.0 50.0 100.0]
           (col->vec (:r (core/dt d :set {:r #dt/e (stat/rescale :x 0 100)})))))))

;; ---------------------------------------------------------------------------
;; stat/winsorize :tail option (mbmisc winsor.bm / winsor.tp)
;; ---------------------------------------------------------------------------

(deftest stat-winsorize-tail-option
  (let [col (-> (ds/->dataset {:x [1.0 2.0 3.0 4.0 100.0]}) :x)]
    (testing ":upper clips only the top tail"
      (is (= [1.0 2.0 3.0 4.0 4.0]
             (col->vec (stat/stat-winsorize col 0.25 {:tail :upper})))))
    (testing ":lower clips only the bottom tail"
      (is (= [2.0 2.0 3.0 4.0 100.0]
             (col->vec (stat/stat-winsorize col 0.25 {:tail :lower})))))
    (testing ":both (and the 2-arity default) clip both tails identically"
      (is (= (col->vec (stat/stat-winsorize col 0.25))
             (col->vec (stat/stat-winsorize col 0.25 {:tail :both})))))
    (testing "unknown :tail throws a structured error"
      (is (= :invalid-tail
             (-> (try (stat/stat-winsorize col 0.25 {:tail :top}) nil
                      (catch clojure.lang.ExceptionInfo e e))
                 ex-data :dt/error))))))

(deftest stat-winsorize-tail-in-dt
  (let [d (ds/->dataset {:x [1.0 2.0 3.0 4.0 100.0]})]
    (is (= [1.0 2.0 3.0 4.0 4.0]
           (col->vec (:w (core/dt d :set {:w #dt/e (stat/winsorize :x 0.25 {:tail :upper})})))))))

;; ---------------------------------------------------------------------------
;; Concise aliases for the new transforms
;; ---------------------------------------------------------------------------

(deftest concise-trim-rescale-aliases
  (let [col (-> (ds/->dataset {:x [1.0 2.0 3.0 4.0 100.0]}) :x)]
    (is (= (col->vec (stat/stat-trim col 0.2)) (col->vec (c/trim col 0.2))))
    (is (= (col->vec (stat/stat-rescale col)) (col->vec (c/rescale col))))))
