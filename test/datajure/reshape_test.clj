(ns datajure.reshape-test
  (:refer-clojure :exclude [cast])
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype.functional :as dfn]
            [datajure.core :as core]
            [datajure.reshape :as reshape :refer [melt cast]]))

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

(deftest cast-round-trip
  (testing "cast is the inverse of melt"
    (let [original (ds/->dataset {:species ["A" "A" "B" "B"]
                                  :year [2007 2008 2007 2008]
                                  :mass [3750 3800 5000 5100]
                                  :flipper [181 182 210 215]})
          melted (melt original {:id [:species :year] :measure [:mass :flipper]})
          casted (cast melted {:id [:species :year] :from :variable :value :value})]
      (is (= #{:species :year :mass :flipper} (set (ds/column-names casted))))
      (is (= [3750 3800 5000 5100] (vec (:mass casted))))
      (is (= [181 182 210 215] (vec (:flipper casted)))))))

(deftest cast-preserves-id-order
  (testing "result rows appear in order of first occurrence of each id-tuple"
    (let [long (ds/->dataset {:id [1 2 1 2] :metric ["a" "a" "b" "b"] :val [10 20 30 40]})
          wide (cast long {:id [:id] :from :metric :value :val})]
      (is (= [1 2] (vec (:id wide))))
      (is (= [10 20] (vec (:a wide))))
      (is (= [30 40] (vec (:b wide)))))))

(deftest cast-fill-missing-cells
  (testing "nil fill for missing (id, from) combinations"
    (let [long (ds/->dataset {:id [1 1 2] :metric ["a" "b" "a"] :val [10 20 30]})
          wide (cast long {:id [:id] :from :metric :value :val})]
      ;; id=2 has no "b" row -> :b should be nil
      (is (= [10 30] (vec (:a wide))))
      (is (= [20 nil] (vec (:b wide))))))
  (testing "custom :fill value for missing cells"
    (let [long (ds/->dataset {:id [1 1 2] :metric ["a" "b" "a"] :val [10 20 30]})
          wide (cast long {:id [:id] :from :metric :value :val :fill 0})]
      (is (= [20 0] (vec (:b wide)))))))

(deftest cast-duplicate-cells-first
  (testing "default: first value wins when (id, from) has duplicates"
    (let [long (ds/->dataset {:id [1 1] :metric ["a" "a"] :val [10 99]})
          wide (cast long {:id [:id] :from :metric :value :val})]
      (is (= [10] (vec (:a wide)))))))

(deftest cast-agg-fn
  (testing ":agg applied to a vector of values per cell"
    (let [long (ds/->dataset {:id [1 1 2 2] :metric ["a" "a" "a" "a"] :val [10.0 20.0 30.0 40.0]})
          wide (cast long {:id [:id] :from :metric :value :val :agg dfn/mean})]
      (is (= [15.0 35.0] (vec (:a wide)))))))

(deftest cast-multi-id
  (testing "multiple :id columns combine to form row identifiers"
    (let [long (ds/->dataset {:sym ["A" "A" "B" "B"]
                              :date [1 1 1 1]
                              :metric ["bid" "ask" "bid" "ask"]
                              :val [10.0 11.0 20.0 21.0]})
          wide (cast long {:id [:sym :date] :from :metric :value :val})]
      (is (= 2 (ds/row-count wide)))
      (is (= ["A" "B"] (vec (:sym wide))))
      (is (= [10.0 20.0] (vec (:bid wide))))
      (is (= [11.0 21.0] (vec (:ask wide)))))))

(deftest cast-keyword-from-values
  (testing ":from column containing keywords produces keyword column names"
    (let [long (ds/->dataset {:id [1 1] :metric [:mass :flipper] :val [3750 181]})
          wide (cast long {:id [:id] :from :metric :value :val})]
      (is (contains? (set (ds/column-names wide)) :mass))
      (is (contains? (set (ds/column-names wide)) :flipper)))))

(deftest cast-pipeline
  (testing "cast result threads into dt"
    (let [long (ds/->dataset {:id [1 1 2 2] :metric ["a" "b" "a" "b"] :val [10 20 30 40]})
          result (-> (cast long {:id [:id] :from :metric :value :val})
                     (core/dt :where #dt/e (> :a 10)))]
      (is (= 1 (ds/row-count result)))
      (is (= [2] (vec (:id result)))))))

(deftest cast-missing-id-error
  (is (thrown? clojure.lang.ExceptionInfo
               (cast (ds/->dataset {:a [1]}) {:from :a :value :a}))))

(deftest cast-missing-from-error
  (is (thrown? clojure.lang.ExceptionInfo
               (cast (ds/->dataset {:a [1]}) {:id [:a] :value :a}))))

(deftest cast-missing-value-error
  (is (thrown? clojure.lang.ExceptionInfo
               (cast (ds/->dataset {:a [1]}) {:id [:a] :from :a}))))

;; ---------------------------------------------------------------------------
;; tsfill — panel gap-filling (Stata tsfill / tidyr complete)
;; ---------------------------------------------------------------------------

(deftest tsfill-numeric-basic
  (let [d (ds/->dataset {:id ["b" "a" "a" "b"] :year [2019 2015 2018 2017] :x [9.0 1.0 4.0 7.0]})
        r (reshape/tsfill d {:by :id :date :year})]
    (testing "per-group grid from each group's own min..max, default step 1"
      (is (= ["a" "a" "a" "a" "b" "b" "b"] (vec (:id r))))
      (is (= [2015 2016 2017 2018 2017 2018 2019] (vec (:year r))))
      (is (= [1.0 nil nil 4.0 7.0 nil 9.0] (vec (:x r)))))
    (testing "column order preserved"
      (is (= [:id :year :x] (vec (ds/column-names r)))))))

(deftest tsfill-temporal-and-carry
  (let [ld #(java.time.LocalDate/parse %)
        d (ds/->dataset {:id ["a" "a" "a"]
                         :m [(ld "2020-01-01") (ld "2020-04-01") (ld "2020-02-15")]
                         :name ["acme" "acme" "acme2"]
                         :x [1.0 4.0 99.0]})
        r (reshape/tsfill d {:by [:id] :date :m :every :month :carry [:name]})]
    (testing "union semantics: the off-grid 02-15 row is KEPT, grid rows inserted"
      (is (= [(ld "2020-01-01") (ld "2020-02-01") (ld "2020-02-15") (ld "2020-03-01") (ld "2020-04-01")]
             (vec (:m r))))
      (is (= [1.0 nil 99.0 nil 4.0] (vec (:x r)))))
    (testing ":carry fills the inserted rows (NOCB→LOCF)"
      (is (= ["acme" "acme2" "acme2" "acme" "acme"] (vec (:name r)))))))

(deftest tsfill-quarter-and-float-step
  (let [ld #(java.time.LocalDate/parse %)]
    (testing ":quarter unit (3 months)"
      (let [d (ds/->dataset {:q [(ld "2020-01-01") (ld "2020-10-01")] :x [1.0 4.0]})]
        (is (= [(ld "2020-01-01") (ld "2020-04-01") (ld "2020-07-01") (ld "2020-10-01")]
               (vec (:q (reshape/tsfill d {:date :q :every :quarter})))))))
    (testing "fractional numeric step (yearqtr-style), no accumulation drift"
      (let [d (ds/->dataset {:q [2020.0 2020.75] :x [1.0 4.0]})]
        (is (= [2020.0 2020.25 2020.5 2020.75]
               (vec (:q (reshape/tsfill d {:date :q :every 0.25})))))))))

(deftest tsfill-explicit-grid
  (testing ":grid clipped to each group's [min,max]; whole dataset when no :by"
    (let [d (ds/->dataset {:t [1 5] :x [10.0 50.0]})
          r (reshape/tsfill d {:date :t :grid [1 2 3 5 8]})]
      (is (= [1 2 3 5] (vec (:t r))))
      (is (= [10.0 nil nil 50.0] (vec (:x r)))))))

(deftest tsfill-nil-dates-and-empty
  (testing "nil-date rows are dropped (documented); empty dataset passes through"
    (let [d (ds/->dataset {:id ["a" "a" "a"] :y [1 nil 3] :x [1.0 2.0 3.0]})]
      (is (= [1 2 3] (vec (:y (reshape/tsfill d {:by :id :date :y}))))))
    (is (zero? (ds/row-count (reshape/tsfill (ds/->dataset {:id [] :y []}) {:by :id :date :y}))))))

(deftest tsfill-errors
  (let [err (fn [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (-> e ex-data :dt/error))))]
    (testing "duplicate (key, date) throws"
      (is (= :tsfill-duplicate-dates
             (err #(reshape/tsfill (ds/->dataset {:id ["a" "a"] :y [1 1] :x [1 2]})
                                   {:by :id :date :y})))))
    (testing "nil group key throws"
      (is (= :tsfill-nil-key
             (err #(reshape/tsfill (ds/->dataset {:id ["a" nil] :y [1 2] :x [1 2]})
                                   {:by :id :date :y})))))
    (testing ":every type mismatches throw"
      (is (= :tsfill-invalid-every
             (err #(reshape/tsfill (ds/->dataset {:y [1 2] :x [1 2]}) {:date :y :every :month}))))
      (is (= :tsfill-invalid-every
             (err #(reshape/tsfill (ds/->dataset {:m [(java.time.LocalDate/now)] :x [1]})
                                   {:date :m :every 2})))))
    (testing "missing :date and unknown columns throw"
      (is (= :tsfill-missing-date
             (err #(reshape/tsfill (ds/->dataset {:y [1]}) {}))))
      (is (= :unknown-column
             (err #(reshape/tsfill (ds/->dataset {:y [1]}) {:date :z})))))))

(deftest tsfill-unit-spellings
  (testing ":every accepts plural spellings (:months == :month)"
    (let [d (ds/->dataset {:k [:a :a]
                           :d [(java.time.LocalDate/of 2024 1 1)
                               (java.time.LocalDate/of 2024 4 1)]})]
      (is (= (vec (:d (reshape/tsfill d {:by [:k] :date :d :every :month})))
             (vec (:d (reshape/tsfill d {:by [:k] :date :d :every :months}))))))))

(deftest reshape-not-a-dataset-errors
  (let [err (fn [f] (try (f) nil (catch clojure.lang.ExceptionInfo e (:dt/error (ex-data e)))))]
    (is (= :not-a-dataset (err #(melt {:a [1]} {:id [:a]}))))
    (is (= :not-a-dataset (err #(cast nil {:id [:a] :from :f :value :v}))))
    (is (= :not-a-dataset (err #(reshape/tsfill [{:a 1}] {:date :a}))))))
