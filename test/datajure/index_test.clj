(ns datajure.index-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.core :as core]
            [datajure.index :as idx]))

;;; ---- fixtures --------------------------------------------------------------

(def panel
  ;; A tiny wide-panel stand-in: several firms, several rows each, out of order.
  (ds/->dataset {:tic   ["AAPL" "MSFT" "AAPL" "GOOG" "MSFT" "AAPL"]
                 :year  [2021    2021   2020   2021   2020   2022]
                 :sales [100     90     80     70     85     120]}))

;;; ---- single-column index ---------------------------------------------------

(deftest single-key-lookup-test
  (testing "lookup returns all rows for a key, in original row order"
    (let [by-tic (idx/index-by panel :tic)
          aapl   (idx/lookup by-tic "AAPL")]
      (is (idx/index? by-tic))
      (is (= 3 (ds/row-count aapl)))
      (is (= ["AAPL" "AAPL" "AAPL"] (vec (aapl :tic))))
      (is (= [2021 2020 2022] (vec (aapl :year))))        ; original order preserved
      (is (= [100 80 120] (vec (aapl :sales)))))))

(deftest single-key-lookup-indices-test
  (testing "lookup-indices returns raw row indices into the source dataset"
    (let [by-tic (idx/index-by panel :tic)]
      (is (= [0 2 5] (vec (idx/lookup-indices by-tic "AAPL"))))
      (is (= [1 4]   (vec (idx/lookup-indices by-tic "MSFT"))))
      (is (= [3]     (vec (idx/lookup-indices by-tic "GOOG")))))))

(deftest missing-key-yields-empty-dataset-test
  (testing "absent key → 0-row dataset with the source columns"
    (let [by-tic (idx/index-by panel :tic)
          none   (idx/lookup by-tic "TSLA")]
      (is (= 0 (ds/row-count none)))
      (is (= #{:tic :year :sales} (set (ds/column-names none))))
      (is (= [] (vec (idx/lookup-indices by-tic "TSLA")))))))

(deftest single-key-accepts-one-tuple-test
  (testing "a single-column index accepts either a scalar or a 1-element tuple"
    (let [by-tic (idx/index-by panel :tic)]
      (is (= (vec (idx/lookup-indices by-tic "MSFT"))
             (vec (idx/lookup-indices by-tic ["MSFT"])))))))

;;; ---- multi-column index ----------------------------------------------------

(deftest multi-key-lookup-test
  (testing "tuple key resolves the exact (tic, year) row"
    (let [by-firm-year (idx/index-by panel [:tic :year])
          r            (idx/lookup by-firm-year ["AAPL" 2020])]
      (is (= 1 (ds/row-count r)))
      (is (= [80] (vec (r :sales))))))
  (testing "absent tuple → empty"
    (let [by-firm-year (idx/index-by panel [:tic :year])]
      (is (= [] (vec (idx/lookup-indices by-firm-year ["GOOG" 1999])))))))

(deftest multi-key-scalar-rejected-test
  (testing "passing a scalar to a multi-column index throws :invalid-lookup-key"
    (let [by-firm-year (idx/index-by panel [:tic :year])
          e (try (idx/lookup by-firm-year "AAPL") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :invalid-lookup-key (:dt/error (ex-data e)))))))

;;; ---- introspection ---------------------------------------------------------

(deftest introspection-test
  (testing "index? and key-columns"
    (let [by-tic (idx/index-by panel :tic)]
      (is (true? (idx/index? by-tic)))
      (is (false? (idx/index? {})))
      (is (false? (idx/index? panel)))
      (is (= [:tic] (idx/key-columns by-tic)))
      (is (= [:tic :year] (idx/key-columns (idx/index-by panel [:tic :year])))))))

;;; ---- validation ------------------------------------------------------------

(deftest unknown-column-test
  (testing "indexing on a missing column throws :unknown-column"
    (let [e (try (idx/index-by panel :nope) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :unknown-column (:dt/error (ex-data e))))
      (is (= [:nope] (:dt/unknown (ex-data e)))))))

(deftest invalid-key-cols-test
  (testing "no key columns / non-keyword key spec throws :invalid-key-cols"
    (is (= :invalid-key-cols
           (:dt/error (try (idx/index-by panel []) nil
                           (catch clojure.lang.ExceptionInfo e (ex-data e))))))
    (is (= :invalid-key-cols
           (:dt/error (try (idx/index-by panel 42) nil
                           (catch clojure.lang.ExceptionInfo e (ex-data e))))))))

(deftest not-an-index-test
  (testing "lookup on a non-index throws :not-an-index"
    (let [e (try (idx/lookup {:not :an-index} "AAPL") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :not-an-index (:dt/error (ex-data e)))))))

;;; ---- composition (the filter-ticker pattern) -------------------------------

(deftest composes-with-dt-test
  (testing "lookup → dt :order-by/:take, mirroring the app's filter-ticker"
    (let [by-tic (idx/index-by panel :tic)
          last2  (-> (idx/lookup by-tic "AAPL")
                     (core/dt :order-by [(core/asc :year)] :take -2))]
      (is (= 2 (ds/row-count last2)))
      (is (= [2021 2022] (vec (last2 :year))))            ; two most recent years
      (is (= [100 120] (vec (last2 :sales)))))))
