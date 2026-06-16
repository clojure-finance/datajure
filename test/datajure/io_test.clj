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

(deftest csv-column-filter-accepts-keywords
  ;; CSV/TSV (charred) match allow/block lists against raw header strings before
  ;; :key-fn, so a keyword allowlist used to silently yield 0 columns. dio/read
  ;; normalises keyword entries to raw names for text formats.
  (testing "keyword :column-allowlist works (the natural datajure form)"
    (let [path (tmp "datajure-test-allow.csv")]
      (dio/write test-ds path)
      (is (= #{:species :mass}
             (set (ds/column-names (dio/read path {:column-allowlist [:species :mass]})))))
      (.delete (File. path))))
  (testing "keyword :column-blocklist works"
    (let [path (tmp "datajure-test-block.csv")]
      (dio/write test-ds path)
      (is (= #{:species :mass}
             (set (ds/column-names (dio/read path {:column-blocklist [:year]})))))
      (.delete (File. path))))
  (testing "string :column-allowlist still works"
    (let [path (tmp "datajure-test-allow-str.csv")]
      (dio/write test-ds path)
      (is (= #{:species :mass}
             (set (ds/column-names (dio/read path {:column-allowlist ["species" "mass"]})))))
      (.delete (File. path)))))

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

(deftest json-round-trip
  (testing "write and read JSON"
    (let [path (tmp "datajure-test.json")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (is (= #{:species :mass :year} (set (ds/column-names result))))
        (.delete (File. path))))))

(deftest json-gz-round-trip
  (testing "write and read gzipped JSON"
    (let [path (tmp "datajure-test.json.gz")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (is (= #{:species :mass :year} (set (ds/column-names result))))
        (.delete (File. path))))))

(deftest jsonl-round-trip
  (testing "write and read JSON Lines"
    (let [path (tmp "datajure-test.jsonl")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (is (= #{:species :mass :year} (set (ds/column-names result))))
        (.delete (File. path))))))

(deftest jsonl-gz-round-trip
  (testing "write and read gzipped JSON Lines"
    (let [path (tmp "datajure-test.jsonl.gz")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (is (= #{:species :mass :year} (set (ds/column-names result))))
        (.delete (File. path))))))

(deftest ndjson-extension-alias
  (testing ".ndjson is treated as JSON Lines"
    (let [path (tmp "datajure-test.ndjson")]
      (dio/write test-ds path)
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))
        (is (= #{:species :mass :year} (set (ds/column-names result))))
        (.delete (File. path))))))

(deftest jsonl-read-seq-streams-in-batches
  (testing "read-seq on JSONL yields multiple chunks sized by :batch-size, in order"
    (let [big  (ds/->dataset {:i (vec (range 10)) :v (mapv #(str "v" %) (range 10))})
          path (tmp "datajure-test-stream.jsonl")]
      (dio/write big path)
      (let [chunks (dio/read-seq path {:batch-size 4})]   ;; 10 rows / 4 -> [4 4 2]
        (is (= 3 (count chunks)))
        (is (= [4 4 2] (mapv ds/row-count chunks)))
        (is (= 10 (reduce + (map ds/row-count chunks))))
        (is (= (vec (range 10)) (vec (mapcat #(seq (% :i)) chunks))))
        ;; reader is released after full realization: a fresh read still works
        (is (= 10 (ds/row-count (dio/read path)))))
      (.delete (File. path)))))

(deftest jsonl-read-seq-default-single-chunk
  (testing "small JSONL fits in one chunk under the default batch size"
    (let [path (tmp "datajure-test-stream1.jsonl")]
      (dio/write test-ds path)
      (is (= 1 (count (dio/read-seq path))))
      (.delete (File. path)))))

(deftest jsonl-sparse-rows-and-blank-lines
  (testing "missing keys become nil; blank lines are skipped"
    (let [path (tmp "datajure-test-sparse.jsonl")]
      (spit path "{\"a\":1,\"b\":\"x\"}\n\n{\"a\":2}\n{\"a\":3,\"b\":\"z\"}\n")
      (let [result (dio/read path)]
        (is (= 3 (ds/row-count result)))            ;; blank line skipped
        (is (= ["x" nil "z"] (vec (result :b)))))    ;; missing :b -> nil
      (.delete (File. path)))))

(deftest json-read-seq
  (testing "read-seq on JSON yields a one-element seq of the whole dataset"
    (let [path (tmp "datajure-test-seq.json")]
      (dio/write test-ds path)
      (let [chunks (dio/read-seq path)]
        (is (= 1 (count chunks)))
        (is (= 3 (ds/row-count (first chunks))))
        (is (= #{:species :mass :year} (set (ds/column-names (first chunks)))))
        (.delete (File. path))))))

(deftest read-seq-unsupported-format
  (testing "read-seq on a non-Parquet/non-JSON format throws :unsupported-format"
    (let [e (try (dio/read-seq "data.csv") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :unsupported-format (-> e ex-data :dt/error)))
      (is (= :csv (-> e ex-data :dt/ext))))))

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
    (let [e (try (dio/read "data.xml") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :unsupported-format (-> e ex-data :dt/error)))
      (is (= :xml (-> e ex-data :dt/ext))))))

(deftest unsupported-format-write
  (testing "unsupported extension on write throws :unsupported-format"
    (let [e (try (dio/write test-ds "output.xml") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :unsupported-format (-> e ex-data :dt/error))))))

(deftest missing-parquet-dep
  (testing "parquet without dep throws :missing-dep"
    (let [e (try (dio/read "data.parquet") nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (when (some? e)
        (is (= :missing-dep (-> e ex-data :dt/error)))))))
