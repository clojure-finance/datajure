(ns datajure.io
  "Unified file I/O for datajure. Dispatches on file extension.
  Natively supports: CSV, TSV, nippy (and .gz variants of all three).
  Parquet, xlsx/xls, and Arrow require optional extra dependencies."
  (:refer-clojure :exclude [read])
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.io :as ds-io]
            [clojure.string :as str]))

(defn- file-ext
  "Extract the effective extension from a path, stripping .gz suffix first.
  Returns a lowercase keyword, e.g. :csv :tsv :parquet :nippy :xlsx."
  [path]
  (let [s (str path)
        s (if (str/ends-with? s ".gz") (subs s 0 (- (count s) 3)) s)
        dot (.lastIndexOf s ".")]
    (when (> dot -1)
      (keyword (str/lower-case (subs s (inc dot)))))))

(defn- try-require-parquet []
  (try (require '[tech.v3.libs.parquet]) true
       (catch Exception _ false)))

(defn- try-require-excel-fast []
  (try (require '[tech.v3.libs.fastexcel]) true
       (catch Exception _ false)))

(defn- try-require-excel-poi []
  (try (require '[tech.v3.libs.poi]) true
       (catch Exception _ false)))

(defn- try-require-arrow []
  (try (require '[tech.v3.libs.arrow]) true
       (catch Exception _ false)))

(defn- require-parquet! []
  (when-not (try-require-parquet)
    (throw (ex-info "Parquet support requires tech.v3.libs.parquet on the classpath."
                    {:dt/error :missing-dep :dt/dep "tech.v3.libs.parquet"}))))

(defn- require-excel! [ext]
  (when-not (or (try-require-excel-fast) (try-require-excel-poi))
    (throw (ex-info (str ext " support requires tech.v3.libs.fastexcel (xlsx) or tech.v3.libs.poi (xls) on the classpath.")
                    {:dt/error :missing-dep :dt/dep "tech.v3.libs.fastexcel or tech.v3.libs.poi"}))))

(defn- require-arrow! []
  (when-not (try-require-arrow)
    (throw (ex-info "Arrow support requires tech.v3.libs.arrow on the classpath."
                    {:dt/error :missing-dep :dt/dep "tech.v3.libs.arrow"}))))

(defn read
  "Read a dataset from a file. Dispatches on file extension.

  Natively supported: .csv .tsv .nippy (and .gz variants).
  Optional deps required: .parquet .xlsx .xls .arrow .feather.

  Options are passed through to the underlying tech.v3.dataset reader.
  Columns are returned as keywords by default (:key-fn keyword).

  Examples:
    (read \"data.csv\")
    (read \"data.parquet\")
    (read \"data.tsv.gz\")
    (read \"data.csv\" {:separator \\tab})"
  ([path] (read path {}))
  ([path options]
   (let [ext (file-ext path)
         opts (merge {:key-fn keyword} options)]
     (case ext
       (:csv :tsv :nippy nil) (ds-io/->dataset path opts)
       :parquet (do (require-parquet!) (ds-io/->dataset path opts))
       :xlsx (do (require-excel! "xlsx") (ds-io/->dataset path opts))
       :xls (do (require-excel! "xls") (ds-io/->dataset path opts))
       (:arrow :feather) (do (require-arrow!)
                             (let [arrow-ns (the-ns 'tech.v3.libs.arrow)]
                               ((ns-resolve arrow-ns 'stream->dataset) path opts)))
       (throw (ex-info (str "Unsupported file extension: " (name ext)
                            ". Supported: csv, tsv, nippy, parquet, xlsx, xls, arrow, feather.")
                       {:dt/error :unsupported-format :dt/ext ext}))))))

(defn read-seq
  "Read a file as a lazy sequence of datasets (for large files).
  Currently supported for Parquet only.

  Example:
    (read-seq \"huge.parquet\")"
  ([path] (read-seq path {}))
  ([path options]
   (let [ext (file-ext path)]
     (case ext
       :parquet (do (require-parquet!)
                    (let [parquet-ns (the-ns 'tech.v3.libs.parquet)]
                      ((ns-resolve parquet-ns 'parquet->ds-seq) path options)))
       (throw (ex-info (str "read-seq is only supported for Parquet files. Got: " (name (or ext "unknown")))
                       {:dt/error :unsupported-format :dt/ext ext}))))))

(defn write
  "Write a dataset to a file. Dispatches on file extension.

  Natively supported: .csv .tsv .nippy (and .gz variants).
  Optional deps required: .parquet .xlsx.

  Options are passed through to the underlying tech.v3.dataset writer.

  Examples:
    (write ds \"output.csv\")
    (write ds \"output.parquet\")
    (write ds \"output.tsv.gz\")
    (write ds \"output.csv\" {:separator \\tab})"
  ([dataset path] (write dataset path {}))
  ([dataset path options]
   (let [ext (file-ext path)]
     (case ext
       (:csv :tsv :nippy nil) (ds-io/write! dataset path options)
       :parquet (do (require-parquet!) (ds-io/write! dataset path options))
       :xlsx (do (require-excel! "xlsx") (ds-io/write! dataset path options))
       (:arrow :feather) (do (require-arrow!)
                             (let [arrow-ns (the-ns 'tech.v3.libs.arrow)]
                               ((ns-resolve arrow-ns 'dataset->stream!) dataset path options)))
       (throw (ex-info (str "Unsupported file extension: " (name ext)
                            ". Supported: csv, tsv, nippy, parquet, xlsx, arrow, feather.")
                       {:dt/error :unsupported-format :dt/ext ext}))))))
