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

(deftest blank->nil-basic
  (let [ds (ds/->dataset {:name ["acme" "  " "" "zed"] :n [1 2 3 4]})
        result (du/blank->nil ds)]
    (testing "empty and whitespace-only strings become missing"
      (is (= ["acme" nil nil "zed"] (vec (result :name)))))
    (testing "non-string columns untouched"
      (is (= [1 2 3 4] (vec (result :n)))))))

(deftest blank->nil-column-subset
  ;; whitespace-only strings — a bare "" is already missing at dataset
  ;; construction (tech.ml.dataset maps it to nil), so it can't distinguish
  ;; cleaned from untouched columns
  (let [ds (ds/->dataset {:a ["  " "x"] :b ["  " "y"]})
        result (du/blank->nil ds :a)]
    (is (= [nil "x"] (vec (result :a))))
    (is (= ["  " "y"] (vec (result :b))))))

(deftest parse-numeric-basic
  (let [ds (ds/->dataset {:px ["$1,234.50" "8" "n/a" "2.5e3"]})
        result (du/parse-numeric ds :px)]
    (testing "currency symbols / separators / junk stripped, unparseable → missing"
      (is (= [1234.5 8.0 nil 2500.0] (vec (result :px)))))
    (testing "mixed whole and fractional values give a float column"
      (is (= :float64 (dtype/elemwise-datatype (result :px)))))))

(deftest parse-numeric-integer-column
  (let [ds (ds/->dataset {:n ["1,200" "8" "-3"]})
        result (du/parse-numeric ds :n)]
    (is (= [1200 8 -3] (vec (result :n))))
    (is (= :int64 (dtype/elemwise-datatype (result :n))))))

(deftest parse-numeric-edge-cases
  (let [ds (ds/->dataset {:x ["hello" "-Inf" "NaN" "1.23e+5x" "2015-06-01" nil]})
        result (du/parse-numeric ds :x)]
    (testing "no-digit, non-finite, mangled-exponent, and date-like strings → missing"
      (is (= [nil nil nil nil nil nil] (vec (result :x)))))))

(deftest parse-numeric-numeric-column-unchanged
  (let [ds (ds/->dataset {:x [1.5 2.5]})
        result (du/parse-numeric ds :x)]
    (is (= [1.5 2.5] (vec (result :x))))
    (is (= :float64 (dtype/elemwise-datatype (result :x))))))

(deftest distinct-rows-basic
  (let [d (ds/->dataset {:id [1 1 2 2 3] :d [:x :y :x :x :z] :v [10 20 30 40 50]})]
    (testing "full-row distinct drops only exact duplicate rows, keeps input order"
      (let [dup (ds/->dataset {:a [1 1 1] :b [2 2 9]})]
        (is (= [[1 2] [1 9]] (mapv vec (ds/rowvecs (du/distinct-rows dup)))))))
    (testing "no duplicates → dataset unchanged"
      (is (= 5 (ds/row-count (du/distinct-rows d)))))
    (testing "key-subset distinct keeps the FIRST row per key with all columns"
      (is (= [[1 :x 10] [2 :x 30] [3 :z 50]]
             (mapv vec (ds/rowvecs (du/distinct-rows d [:id]))))))
    (testing "single keyword key is accepted"
      (is (= 3 (ds/row-count (du/distinct-rows d :id)))))
    (testing "multi-column key"
      (is (= 4 (ds/row-count (du/distinct-rows d [:id :d])))))))

(deftest ensure-dataset!-validation
  (testing "returns a real dataset unchanged"
    (let [d (ds/->dataset {:a [1]})]
      (is (identical? d (#'du/ensure-dataset! d "test")))))
  (testing "shape-aware hints: nil / plain map / seq-of-maps / other class"
    (letfn [(msg [x] (try (#'du/ensure-dataset! x "f") nil
                          (catch clojure.lang.ExceptionInfo e (.getMessage e))))]
      (is (re-find #"got nil" (msg nil)))
      (is (re-find #"plain map" (msg {:a [1]})))
      (is (re-find #"sequence of maps" (msg [{:a 1}])))
      (is (re-find #"java\.lang\.String" (msg "x")))))
  (testing "an explicit hint overrides the shape-derived one"
    (is (re-find #"custom hint"
                 (try (#'du/ensure-dataset! nil "f" "custom hint") nil
                      (catch clojure.lang.ExceptionInfo e (.getMessage e))))))
  (testing "ex-data carries :dt/error, :dt/fn, :dt/got"
    (let [d (try (#'du/ensure-dataset! {:a [1]} "myfn") nil
                 (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :not-a-dataset (:dt/error d)))
      (is (= "myfn" (:dt/fn d)))
      (is (= "clojure.lang.PersistentArrayMap" (:dt/got d)))))
  (testing "util entry points are guarded"
    (letfn [(err [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (:dt/error (ex-data e)))))]
      (is (= :not-a-dataset (err #(du/describe {:a [1]}))))
      (is (= :not-a-dataset (err #(du/distinct-rows nil))))
      (is (= :not-a-dataset (err #(du/duplicate-rows nil))))
      (is (= :not-a-dataset (err #(du/mark-duplicates nil))))
      (is (= :not-a-dataset (err #(du/clean-column-names [{:a 1}]))))
      (is (= :not-a-dataset (err #(du/drop-constant-columns nil))))
      (is (= :not-a-dataset (err #(du/coerce-columns nil {:a :int64}))))
      (is (= :not-a-dataset (err #(du/blank->nil nil))))
      (is (= :not-a-dataset (err #(du/parse-numeric {:a ["1"]} :a)))))))
