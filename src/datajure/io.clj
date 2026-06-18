(ns datajure.io
  "Unified file I/O for datajure. Dispatches on file extension.
  Natively supports: CSV, TSV, JSON, JSON Lines (.jsonl/.ndjson), nippy
  (and .gz variants of all). Parquet, xlsx/xls, and Arrow require optional
  extra dependencies."
  (:refer-clojure :exclude [read])
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.io :as ds-io]
            [charred.api :as charred]
            [clojure.string :as str])
  (:import [java.io FileInputStream FileOutputStream InputStreamReader
            OutputStreamWriter BufferedReader BufferedWriter]
           [java.util.zip GZIPInputStream GZIPOutputStream]
           [java.nio.charset StandardCharsets]))

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

;; --- JSON Lines (.jsonl / .ndjson) -----------------------------------------
;; One JSON object per line. Unlike a JSON array, this is genuinely
;; chunk-able, so read-seq can stream batches of rows for files larger than
;; memory. charred (the same parser tech.v3.dataset uses) handles each line.

(def ^:private default-jsonl-batch
  "Rows per chunk for streaming JSONL reads. Override with :batch-size."
  100000)

(defn- jsonl-reader
  "BufferedReader over path, UTF-8, decompressing a .gz suffix transparently."
  ^BufferedReader [path]
  (let [s (str path)
        in (FileInputStream. s)
        in (if (str/ends-with? (str/lower-case s) ".gz") (GZIPInputStream. in) in)]
    (BufferedReader. (InputStreamReader. in StandardCharsets/UTF_8))))

(defn- jsonl-writer
  "BufferedWriter over path, UTF-8, gzip-compressing a .gz suffix transparently."
  ^BufferedWriter [path]
  (let [s (str path)
        out (FileOutputStream. s)
        out (if (str/ends-with? (str/lower-case s) ".gz") (GZIPOutputStream. out) out)]
    (BufferedWriter. (OutputStreamWriter. out StandardCharsets/UTF_8))))

(defn- read-jsonl
  "Read a whole .jsonl(.gz) file into a single dataset. Blank lines are skipped."
  [path opts]
  (let [key-fn (get opts :key-fn keyword)
        ds-opts (dissoc opts :key-fn :batch-size)]
    (with-open [rdr (jsonl-reader path)]
      (ds/->dataset (->> (line-seq rdr)
                         (remove str/blank?)
                         (mapv #(charred/read-json % :key-fn key-fn)))
                    ds-opts))))

(defn- jsonl-ds-seq
  "Lazy sequence of datasets, each built from a batch of :batch-size rows
  (default 100000). Genuinely streaming. The underlying reader is closed when
  the sequence is fully realised — consume it completely (or inside a doseq) so
  the file handle is released."
  [path opts]
  (let [key-fn (get opts :key-fn keyword)
        batch (max 1 (long (get opts :batch-size default-jsonl-batch)))
        ds-opts (dissoc opts :key-fn :batch-size)
        rdr (jsonl-reader path)
        parse (fn [line] (charred/read-json line :key-fn key-fn))]
    ((fn step [lines]
       (lazy-seq
        (let [batch-lines (vec (take batch lines))]
          (if (empty? batch-lines)
            (do (.close rdr) nil)
            (cons (ds/->dataset (mapv parse batch-lines) ds-opts)
                  (step (drop batch lines)))))))
     (remove str/blank? (line-seq rdr)))))

(defn- write-jsonl!
  "Write a dataset as JSON Lines — one JSON object per row per line.
  (Dataset-writer options don't apply to the per-line JSON encoding.)"
  [dataset path]
  (with-open [w (jsonl-writer path)]
    (doseq [row (ds/rows dataset)]
      (.write w ^String (charred/write-json-str row))
      (.write w "\n"))))

(def ^:private column-filter-keys
  "Column allow/block-list option keys. Which form (string vs keyword) matches
  depends on whether the format filters before or after `:key-fn` — see the two
  normalisers below."
  [:column-allowlist :column-blocklist :column-whitelist :column-blacklist])

(defn- normalize-text-column-filters
  "For CSV/TSV, tech.ml.dataset/charred matches column allow/block lists against
  the raw header strings *before* `:key-fn` runs. datajure forces `:key-fn
  keyword`, so a keyword allowlist (the natural datajure form) would silently
  match nothing → a 0-column dataset. Convert keyword entries in those options to
  their raw names so both `[:a :b]` and `[\"a\" \"b\"]` work. Scoped to CSV/TSV
  only — Parquet/Arrow match after `:key-fn` (see normalize-keyword-column-filters)."
  [opts]
  (reduce (fn [o k]
            (if-let [xs (get o k)]
              (assoc o k (mapv #(if (keyword? %) (name %) %) xs))
              o))
          opts
          column-filter-keys))

(defn- normalize-keyword-column-filters
  "For Parquet/Arrow, tech.ml.dataset applies `:key-fn` to the column names
  *before* matching allow/block lists, so a string entry won't match the
  keyworded names (datajure forces `:key-fn keyword`) → a silent 0-column
  dataset. Convert string entries in those options to keywords so both `[:a :b]`
  and `[\"a\" \"b\"]` work — the mirror image of normalize-text-column-filters."
  [opts]
  (reduce (fn [o k]
            (if-let [xs (get o k)]
              (assoc o k (mapv #(if (string? %) (keyword %) %) xs))
              o))
          opts
          column-filter-keys))

(defn- parquet-read-opts
  "Option prep for the Parquet streaming path (`read-seq`): default
  `:key-fn keyword` (datajure's column convention) then normalise string
  column-allow/blocklist entries to keywords. Mirrors what `read` does inline
  for one-shot Parquet/Arrow reads, so a string `:column-allowlist` matches the
  keyworded columns on `read-seq` too (the 2.0.13 fix originally reached `read`
  only — without this, a string allowlist silently projects to 0 columns on the
  streaming reader)."
  [options]
  (normalize-keyword-column-filters (merge {:key-fn keyword} options)))

(defn read
  "Read a dataset from a file. Dispatches on file extension.

  Natively supported: .csv .tsv .json .jsonl/.ndjson .nippy (and .gz variants).
  Optional deps required: .parquet .xlsx .xls .arrow .feather.

  Options are passed through to the underlying tech.v3.dataset reader.
  Columns are returned as keywords by default (:key-fn keyword). :column-allowlist
  / :column-blocklist accept either keywords or strings regardless of format:
  CSV/TSV match raw headers before :key-fn (keywords are normalised to names),
  while Parquet/Arrow match after :key-fn (strings are normalised to keywords).

  Examples:
    (read \"data.csv\")
    (read \"data.json\")
    (read \"data.jsonl\")        ;; JSON Lines — one object per line
    (read \"data.parquet\")
    (read \"data.tsv.gz\")
    (read \"data.csv\" {:separator \\tab})
    (read \"data.csv\" {:column-allowlist [:a :b]})  ;; keyword allowlist OK for CSV/TSV"
  ([path] (read path {}))
  ([path options]
   (let [ext (file-ext path)
         opts (merge {:key-fn keyword} options)]
     (case ext
       (:csv :tsv) (ds-io/->dataset path (normalize-text-column-filters opts))
       (:json :nippy nil) (ds-io/->dataset path opts)
       (:jsonl :ndjson) (read-jsonl path opts)
       :parquet (do (require-parquet!) (ds-io/->dataset path (normalize-keyword-column-filters opts)))
       :xlsx (do (require-excel! "xlsx") (ds-io/->dataset path opts))
       :xls (do (require-excel! "xls") (ds-io/->dataset path opts))
       (:arrow :feather) (do (require-arrow!)
                             (let [arrow-ns (the-ns 'tech.v3.libs.arrow)]
                               ((ns-resolve arrow-ns 'stream->dataset) path (normalize-keyword-column-filters opts))))
       (throw (ex-info (str "Unsupported file extension: " (name ext)
                            ". Supported: csv, tsv, json, jsonl, ndjson, nippy, parquet, xlsx, xls, arrow, feather.")
                       {:dt/error :unsupported-format :dt/ext ext}))))))

(defn read-seq
  "Read a file as a lazy sequence of datasets.

  - Parquet streams in row-group chunks — genuinely incremental, suitable for
    files larger than memory.
  - JSON Lines (.jsonl / .ndjson, and .gz variants) streams in batches of
    :batch-size rows (default 100000) — also genuinely incremental. Fully
    consume the sequence (e.g. inside a doseq) so the underlying file handle
    is released.
  - JSON (.json) is a single array-of-objects document with no chunk
    boundaries, so it is read whole and yielded as a one-element lazy sequence.
    This gives no streaming/memory benefit — it exists only so a `read-seq`
    call site works uniformly across formats. Reach for Parquet or JSON Lines
    for true out-of-core reads.

  Columns are keywords by default (`:key-fn keyword`), and `:column-allowlist`
  / `:column-blocklist` accept either keywords or strings — matching `read`.

  Examples:
    (read-seq \"huge.parquet\")                      ;; many chunks, streamed
    (read-seq \"huge.jsonl\" {:batch-size 50000})    ;; streamed in 50k-row chunks
    (doseq [chunk (read-seq \"data.json\")]           ;; exactly one chunk
      (process chunk))"
  ([path] (read-seq path {}))
  ([path options]
   (let [ext (file-ext path)]
     (case ext
       :parquet (do (require-parquet!)
                    (let [parquet-ns (the-ns 'tech.v3.libs.parquet)]
                      ((ns-resolve parquet-ns 'parquet->ds-seq) path (parquet-read-opts options))))
       (:jsonl :ndjson) (jsonl-ds-seq path options)
       :json (lazy-seq [(read path options)])
       (throw (ex-info (str "read-seq supports Parquet and JSON Lines (streamed) and JSON (single chunk). Got: " (name (or ext "unknown")))
                       {:dt/error :unsupported-format :dt/ext ext}))))))

(defn write
  "Write a dataset to a file. Dispatches on file extension.

  Natively supported: .csv .tsv .json .jsonl/.ndjson .nippy (and .gz variants).
  Optional deps required: .parquet .xlsx.

  Options are passed through to the underlying tech.v3.dataset writer
  (JSON Lines encodes each row independently and ignores writer options).

  Examples:
    (write ds \"output.csv\")
    (write ds \"output.json\")
    (write ds \"output.jsonl\")     ;; JSON Lines — one object per line
    (write ds \"output.parquet\")
    (write ds \"output.tsv.gz\")
    (write ds \"output.csv\" {:separator \\tab})"
  ([dataset path] (write dataset path {}))
  ([dataset path options]
   (let [ext (file-ext path)]
     (case ext
       (:csv :tsv :json :nippy nil) (ds-io/write! dataset path options)
       (:jsonl :ndjson) (write-jsonl! dataset path)
       :parquet (do (require-parquet!) (ds-io/write! dataset path options))
       :xlsx (do (require-excel! "xlsx") (ds-io/write! dataset path options))
       (:arrow :feather) (do (require-arrow!)
                             (let [arrow-ns (the-ns 'tech.v3.libs.arrow)]
                               ((ns-resolve arrow-ns 'dataset->stream!) dataset path options)))
       (throw (ex-info (str "Unsupported file extension: " (name ext)
                            ". Supported: csv, tsv, json, jsonl, ndjson, nippy, parquet, xlsx, arrow, feather.")
                       {:dt/error :unsupported-format :dt/ext ext}))))))
