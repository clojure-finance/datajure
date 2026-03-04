(ns datajure.io-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.io :as dio])
  (:import java.io.File))

(def ^:private test-ds
  (ds/->dataset {:species ["Adelie" "Gentoo" "Chinstrap"]
                 :mass [3750 5000 3500]
                 :year [2007 2008 2007]}))

(defn- tmp [filename]
  (str (System/getProperty "java.io.tmpdir") "/" filename))

(deftest csv-round-trip
  (testing "write and read CSV"
    (let [path (tmp "datajure-test.csv")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (is (= #{:species :mass :year} (set (ds/column-names result))))
        (.delete (File. path))))))

(deftest csv-gz-round-trip
  (testing "write and read gzipped CSV"
    (let [path (tmp "datajure-test.csv.gz")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (is (= #{:species :mass :year} (set (ds/column-names result))))
        (.delete (File. path))))))

(deftest tsv-round-trip
  (testing "write and read TSV"
    (let [path (tmp "datajure-test.tsv")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (.delete (File. path))))))

(deftest tsv-gz-round-trip
  (testing "write and read gzipped TSV"
    (let [path (tmp "datajure-test.tsv.gz")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (.delete (File. path))))))

(deftest nippy-round-trip
  (testing "write and read nippy"
    (let [path (tmp "datajure-test.nippy")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (is (= #{:species :mass :year} (set (ds/column-names result))))
        (.delete (File. path))))))

(deftest keyword-columns-by-default
  (testing "read returns keyword column names by default"
    (let [path (tmp "datajure-test-kw.csv")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (every? keyword? (ds/column-names result)))
        (.delete (File. path))))))

(deftest read-options-passthrough
  (testing "options are passed through to underlying reader"
    (let [path (tmp "datajure-test-opts.csv")]
      (dio/write test-ds path)
      (let [result (dio/read path {:n-initial-skip-rows 0})]
        (is (= 3 (ds/row-count result)))
        (.delete (File. path))))))

(deftest unsupported-format-read
  (testing "unsupported extension throws :unsupported-format"
    (let [e (try (dio/read "data.json") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :unsupported-format (-> e ex-data :dt/error)))
      (is (= :json (-> e ex-data :dt/ext))))))

(deftest unsupported-format-write
  (testing "unsupported extension on write throws :unsupported-format"
    (let [e (try (dio/write test-ds "output.json") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :unsupported-format (-> e ex-data :dt/error))))))

(deftest missing-parquet-dep
  (testing "parquet without dep throws :missing-dep"
    (let [e (try (dio/read "data.parquet") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (when (some? e)
        (is (= :missing-dep (-> e ex-data :dt/error)))))))
