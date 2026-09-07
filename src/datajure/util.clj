(ns datajure.util
  "Data cleaning and exploration utilities for datajure.
  Standalone functions that operate on datasets directly and thread naturally.
  Not part of dt — these complement it for common data preparation tasks."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.casting :as casting]
            [datajure.math :as math]
            [clojure.string :as str]))

(defn- ensure-dataset!
  "Internal (other datajure namespaces call it via the var). Validate that `x`
  is a tech.v3.dataset, returning it unchanged. Otherwise
  throws a structured :not-a-dataset error whose message names the calling
  function (`fn-name`, e.g. \"dt\" or \"join (left dataset)\") and carries a
  shape-aware hint: nil, a plain column map, and a sequence of maps each get a
  targeted suggestion; anything else reports its class. `hint` (optional)
  overrides the shape-derived hint — used by dt to detect a query map passed
  where the dataset belongs."
  ([x fn-name] (ensure-dataset! x fn-name nil))
  ([x fn-name hint]
   (when-not (ds/dataset? x)
     (let [hint (or hint
                    (cond
                      (nil? x)
                      "got nil — did an upstream load or step return nil?"

                      (map? x)
                      "got a plain map — construct a dataset with (tech.v3.dataset/->dataset {...})"

                      (and (sequential? x) (seq x) (every? map? (take 8 x)))
                      "got a sequence of maps — construct a dataset with (tech.v3.dataset/->dataset [...])"

                      :else
                      (str "got " (.getName (class x)))))]
       (throw (ex-info (str fn-name " expects a dataset — " hint)
                       {:dt/error :not-a-dataset
                        :dt/fn fn-name
                        :dt/got (some-> x class .getName)}))))
   x))

(defn- describe-column [dataset col-kw]
  (let [col (dataset col-kw)
        dt (dtype/elemwise-datatype col)
        n (dtype/ecount col)
        n-missing (dtype/ecount (ds/missing col))
        numeric? (casting/numeric-type? dt)
        ;; Skip dfn stats if the column has no observable values (all-missing or
        ;; empty). Otherwise dfn/standard-deviation on an all-NaN :float64 column
        ;; returns -0.0 even though mean/min/max correctly return nil, yielding
        ;; an incoherent summary row.
        all-missing? (or (zero? n) (= n-missing n))]
    (if (and numeric? (not all-missing?))
      ;; R type-7 quartiles (consistent with median / qnt elsewhere in datajure)
      {:column col-kw
       :datatype dt
       :n n
       :n-missing n-missing
       :mean (dfn/mean col)
       :sd (dfn/standard-deviation col)
       :min (dfn/reduce-min col)
       :p25 (math/quantile-type7 col 0.25)
       :median (math/quantile-type7 col 0.5)
       :p75 (math/quantile-type7 col 0.75)
       :max (dfn/reduce-max col)}
      {:column col-kw
       :datatype dt
       :n n
       :n-missing n-missing
       :mean nil :sd nil :min nil :p25 nil :median nil :p75 nil :max nil})))

(defn describe
  "Descriptive statistics for dataset columns. Returns a dataset with one row
  per column: :column, :datatype, :n, :n-missing, :mean, :sd, :min, :p25,
  :median, :p75, :max. Non-numeric columns show nil for stats.
  Optional second arg selects columns (vector of keywords)."
  ([dataset]
   (ensure-dataset! dataset "describe")
   (describe dataset (vec (ds/column-names dataset))))
  ([dataset cols]
   (ensure-dataset! dataset "describe")
   (let [cols (if (keyword? cols) [cols] cols)]
     (ds/->dataset (mapv #(describe-column dataset %) cols)))))

(defn clean-column-names
  "Clean column names: lowercase, replace any run of non-letter/non-digit characters
  with a single hyphen, collapse consecutive hyphens, strip leading/trailing hyphens.
  Unicode-aware: preserves letters and digits in all scripts (CJK, Cyrillic, Greek,
  accented Latin, etc). Only punctuation, whitespace, and symbols are replaced.

  \"Some Ugly Name!\" → :some-ugly-name
  \"市值 (HKD millions)!\" → :市值-hkd-millions"
  [dataset]
  (ensure-dataset! dataset "clean-column-names")
  (let [col-names (ds/column-names dataset)
        rename-map (into {}
                         (map (fn [col]
                                (let [clean (-> (name col)
                                                str/lower-case
                                                (str/replace #"[^\p{L}\p{N}]+" "-")
                                                (str/replace #"-+" "-")
                                                (str/replace #"^-|-$" ""))]
                                  [col (keyword clean)])))
                         col-names)]
    (ds/rename-columns dataset rename-map)))

(defn duplicate-rows
  "Returns dataset of duplicate rows only. Optional second arg specifies
  subset of columns to check for duplicates."
  ([dataset]
   (duplicate-rows dataset (vec (ds/column-names dataset))))
  ([dataset cols]
   (ensure-dataset! dataset "duplicate-rows")
   (let [cols (if (keyword? cols) [cols] cols)
         grouped (group-by (fn [idx]
                             (mapv #(nth (dataset %) idx) cols))
                           (range (ds/row-count dataset)))
         dup-indices (->> grouped
                          vals
                          (filter #(> (count %) 1))
                          (mapcat identity)
                          sort
                          vec)]
     (ds/select-rows dataset dup-indices))))

(defn distinct-rows
  "Distinct rows, keeping the FIRST occurrence of each duplicate group and
  preserving input row order (data.table `unique(DT)` / dplyr `distinct`).
  Optional second arg restricts the duplicate check to a subset of columns
  (a keyword or vector of keywords) — the kept row is the first row of each
  key group, with ALL columns retained. Complements `duplicate-rows` (which
  returns the duplicated rows instead)."
  ([dataset]
   (distinct-rows dataset (vec (ds/column-names dataset))))
  ([dataset cols]
   (ensure-dataset! dataset "distinct-rows")
   (let [cols (if (keyword? cols) [cols] cols)
         seen (java.util.HashSet.)
         keep-indices (filterv (fn [idx]
                                 (.add seen (mapv #(nth (dataset %) idx) cols)))
                               (range (ds/row-count dataset)))]
     (ds/select-rows dataset keep-indices))))

(defn mark-duplicates
  "Adds :duplicate? boolean column. Optional second arg specifies
  subset of columns to check for duplicates."
  ([dataset]
   (mark-duplicates dataset (vec (ds/column-names dataset))))
  ([dataset cols]
   (ensure-dataset! dataset "mark-duplicates")
   (let [cols (if (keyword? cols) [cols] cols)
         grouped (group-by (fn [idx]
                             (mapv #(nth (dataset %) idx) cols))
                           (range (ds/row-count dataset)))
         dup-set (into #{}
                       (mapcat identity)
                       (filter #(> (count %) 1) (vals grouped)))
         markers (mapv #(contains? dup-set %) (range (ds/row-count dataset)))]
     (ds/add-column dataset (ds/new-column :duplicate? markers)))))

(defn drop-constant-columns
  "Remove columns where all values are identical (zero variance).
  Note: columns with 0 or 1 rows are always kept — a single observation has no
  variance by definition, but that does not mean the column is constant across
  observations."
  [dataset]
  (ensure-dataset! dataset "drop-constant-columns")
  (let [keep-cols (filterv (fn [col-kw]
                             (let [col (dtype/->reader (dataset col-kw))
                                   n (dtype/ecount col)]
                               (if (< n 2)
                                 true
                                 (let [first-val (nth col 0)]
                                   (not (every? #(= first-val (nth col %))
                                                (range 1 n)))))))
                           (ds/column-names dataset))]
    (ds/select-columns dataset keep-cols)))

(defn coerce-columns
  "Bulk type coercion. col-type-map is {col-kw datatype-kw ...}.
  Example: (coerce-columns ds {:year :int64 :mass :float64})"
  [dataset col-type-map]
  (ensure-dataset! dataset "coerce-columns")
  (reduce-kv (fn [ds col-kw target-type]
               (ds/update-column ds col-kw #(dtype/elemwise-cast % target-type)))
             dataset
             col-type-map))

(defn blank->nil
  "Convert empty and whitespace-only strings to missing values (mbmisc `emq2na`).
  By default cleans every :string/:text column; an optional second arg restricts
  the cleaning to specific columns (a keyword or vector of keywords). Non-string
  values in the selected columns pass through unchanged."
  ([dataset]
   (ensure-dataset! dataset "blank->nil")
   (blank->nil dataset
               (filterv (fn [c] (contains? #{:string :text}
                                           (dtype/elemwise-datatype (ds/column dataset c))))
                        (ds/column-names dataset))))
  ([dataset cols]
   (ensure-dataset! dataset "blank->nil")
   (let [cols (if (keyword? cols) [cols] cols)]
     (reduce (fn [d c]
               (let [rdr (dtype/->reader (ds/column d c))
                     vals (mapv (fn [v] (when-not (and (string? v) (str/blank? v)) v))
                                rdr)]
                 (ds/add-or-update-column d (ds/new-column c vals))))
             dataset
             cols))))

(defn- parse-numeric-value
  "Leniently parse one value to a finite Double, or nil (mbmisc `destring`).
  Numbers pass through as doubles. Strings are first tried verbatim (which
  handles clean values including exponent notation like \"1.23e+5\"); on
  failure, irrelevant characters (currency symbols, thousands separators,
  units, footnote letters) are stripped and the remainder parsed. A string
  that mixes exponent notation with junk is nil rather than silently
  mangled, as are non-finite spellings (\"Inf\", \"NaN\") and strings with
  no digits at all."
  [v]
  (cond
    (nil? v) nil
    (number? v) (double v)
    :else
    (let [s (str/trim (str v))
          direct (try (Double/parseDouble s) (catch Exception _ nil))
          d (or direct
                ;; stripping would corrupt an exponent ("1.23e+5x" → "1.235") —
                ;; refuse instead of guessing
                (when-not (re-find #"[0-9][eE][+-]?[0-9]" s)
                  (let [cleaned (-> s
                                    (str/replace #"[^0-9.\-]" "")
                                    (str/replace #"\.{2,}" "."))]
                    (when (re-find #"[0-9]" cleaned)
                      (try (Double/parseDouble cleaned) (catch Exception _ nil))))))]
      (when (and d (Double/isFinite d)) d))))

(defn parse-numeric
  "Leniently parse string columns to numbers (mbmisc `destring`): strips
  currency symbols, thousands separators, and trailing junk (\"$50,762.83a\"
  → 50762.83), leaving unparseable values missing. `cols` is a keyword or
  vector of keywords; already-numeric columns are left unchanged. A column
  whose parsed values are all whole numbers becomes an integer column,
  otherwise float.
  Example: (parse-numeric ds [:price :volume])"
  [dataset cols]
  (ensure-dataset! dataset "parse-numeric")
  (let [cols (if (keyword? cols) [cols] cols)]
    (reduce
     (fn [d c]
       (let [col (ds/column d c)]
         (if (casting/numeric-type? (dtype/elemwise-datatype col))
           d
           (let [parsed (mapv parse-numeric-value (dtype/->reader col))
                 present (remove nil? parsed)
                 ;; 2^53: largest range where doubles represent integers exactly
                 ints? (and (seq present)
                            (every? (fn [^double x]
                                      (and (== x (Math/rint x))
                                           (<= (Math/abs x) 9.007199254740992E15)))
                                    present))
                 vals (if ints? (mapv #(some-> ^double % long) parsed) parsed)]
             (ds/add-or-update-column d (ds/new-column c vals))))))
     dataset
     cols)))
