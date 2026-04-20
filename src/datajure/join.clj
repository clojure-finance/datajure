(ns datajure.join
  "Join functions for datajure. Wraps tech.v3.dataset.join/pd-merge for
   regular joins and datajure.asof for as-of and window joins."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.join :as ds-join]
            [tech.v3.datatype.casting :as casting]
            [tech.v3.datatype.datetime :as dtype-dt]
            [datajure.asof :as asof]
            [datajure.expr :as expr]))

(defn- has-duplicate-keys?
  [dataset key-cols]
  (let [keys-ds (ds/select-columns dataset (vec key-cols))
        n-rows (ds/row-count keys-ds)
        n-unique (ds/row-count (ds/unique-by keys-ds identity))]
    (not= n-rows n-unique)))

(defn- key-tuples
  [dataset cols]
  (let [readers (mapv #(ds/column dataset %) cols)
        n (ds/row-count dataset)]
    (set (for [i (range n)]
           (mapv #(nth % i) readers)))))

(defn- print-report
  [left right left-keys right-keys]
  (let [l-tuples (key-tuples left left-keys)
        r-tuples (key-tuples right right-keys)
        n-matched (count (clojure.set/intersection l-tuples r-tuples))
        n-left-only (count (clojure.set/difference l-tuples r-tuples))
        n-right-only (count (clojure.set/difference r-tuples l-tuples))]
    (println (format "[datajure] join report: %d matched, %d left-only, %d right-only"
                     n-matched n-left-only n-right-only))))

(defn- normalize-keys
  "Normalize a keyword or vector of keywords to a vector."
  [k]
  (when k (if (keyword? k) [k] (vec k))))

(defn- numeric-column?
  "Returns true iff the column's :datatype is a numeric type per
  tech.v3.datatype.casting/numeric-type?. Used to gate :tolerance support."
  [dataset col-kw]
  (let [dt (some-> (ds/column dataset col-kw) meta :datatype)]
    (boolean (and dt (casting/numeric-type? dt)))))

(defn- check-tolerance-compatible!
  "Throw a structured ex-info if :tolerance is non-nil but the asof key
  column (last of left-keys / right-keys) is not numeric. Prevents the raw
  ClassCastException that would otherwise surface from (double dt-value)
  inside asof/within-tolerance? for LocalDateTime and other non-numeric types."
  [tolerance left right left-keys right-keys]
  (when (some? tolerance)
    (let [left-asof (peek left-keys)
          right-asof (peek right-keys)]
      (when-not (and (numeric-column? left left-asof)
                     (numeric-column? right right-asof))
        (throw (ex-info
                (str ":tolerance on an as-of join requires a numeric asof key. "
                     "Got left " left-asof " (datatype "
                     (some-> (ds/column left left-asof) meta :datatype) ") "
                     "and right " right-asof " (datatype "
                     (some-> (ds/column right right-asof) meta :datatype) "). "
                     "Convert the asof column to epoch-milliseconds (or another "
                     "numeric representation) before joining, or omit :tolerance.")
                {:dt/error :join-tolerance-non-numeric
                 :dt/tolerance tolerance
                 :dt/left-asof-col left-asof
                 :dt/right-asof-col right-asof
                 :dt/left-asof-datatype (some-> (ds/column left left-asof) meta :datatype)
                 :dt/right-asof-datatype (some-> (ds/column right right-asof) meta :datatype)}))))))

;;; ---- Window join helpers ---------------------------------------------------

(def ^:private window-unit-millis
  "Milliseconds per temporal unit — used to convert window offsets."
  {:seconds dtype-dt/milliseconds-in-second
   :minutes dtype-dt/milliseconds-in-minute
   :hours dtype-dt/milliseconds-in-hour
   :days dtype-dt/milliseconds-in-day
   :weeks dtype-dt/milliseconds-in-week})

(defn- parse-window-spec
  "Parse a window spec vector into {:lo lo-raw :hi hi-raw} in raw units.
  Accepted formats:
    [lo hi]       - raw numeric offsets (no unit conversion)
    [lo hi unit]  - lo and hi in the given temporal unit (e.g. :minutes)
    [lo unit hi]  - alternative ordering (as in the spec examples)

  Throws :join-missing-window for nil input, :join-invalid-window for malformed
  shapes (wrong length, non-numeric endpoints, missing/misplaced unit), and
  :join-unknown-window-unit for unrecognised temporal units."
  [wspec]
  (cond
    (nil? wspec)
    (throw (ex-info ":how :window requires a :window spec, e.g. [-5 0 :minutes] or [-5 0]"
                    {:dt/error :join-missing-window}))
    (not (sequential? wspec))
    (throw (ex-info (str "Window spec must be a vector, got: " (pr-str wspec))
                    {:dt/error :join-invalid-window :dt/window wspec}))
    (not (#{2 3} (count wspec)))
    (throw (ex-info (str "Window spec must have 2 or 3 elements (got " (count wspec)
                         "): " (pr-str wspec)
                         ". Valid shapes: [lo hi], [lo hi unit], [lo unit hi].")
                    {:dt/error :join-invalid-window :dt/window wspec}))
    :else
    (let [[a b c] wspec
          bad-endpoints (fn []
                          (throw (ex-info (str "Window spec requires numeric endpoints, got: "
                                               (pr-str wspec))
                                          {:dt/error :join-invalid-window :dt/window wspec})))
          resolve-unit (fn [unit]
                         (or (window-unit-millis unit)
                             (throw (ex-info (str "Unknown window unit: " unit
                                                  ". Must be :seconds, :minutes, :hours, :days, or :weeks.")
                                             {:dt/error :join-unknown-window-unit :unit unit}))))]
      (cond
        ;; [lo hi]
        (nil? c)
        (if (and (number? a) (number? b))
          {:lo a :hi b}
          (bad-endpoints))
        ;; [lo unit hi]
        (keyword? b)
        (if (and (number? a) (number? c))
          (let [m (resolve-unit b)] {:lo (* a m) :hi (* c m)})
          (bad-endpoints))
        ;; [lo hi unit]
        (keyword? c)
        (if (and (number? a) (number? b))
          (let [m (resolve-unit c)] {:lo (* a m) :hi (* b m)})
          (bad-endpoints))
        :else
        (throw (ex-info (str "Window spec with 3 elements must have the unit as a keyword "
                             "in position 2 or 3 (i.e. [lo unit hi] or [lo hi unit]), got: "
                             (pr-str wspec))
                        {:dt/error :join-invalid-window :dt/window wspec}))))))

(defn- compile-agg-fn
  "Compile a window-join agg value to a fn [sub-dataset] -> scalar.
  For #dt/e expr-nodes: wraps with nil-on-empty guard (dfn functions like
  dfn/mean return NaN on empty columns; we return nil instead).
  For plain fns: used as-is (so nrow returns 0 for empty windows naturally)."
  [f]
  (if (expr/expr-node? f)
    (let [compiled (expr/compile-expr f)]
      (fn [sub-ds]
        (if (zero? (ds/row-count sub-ds))
          nil
          (compiled sub-ds))))
    f))

(defn- apply-window-join
  "Window join: for each left row, aggregate all right rows whose asof-key
  falls within [left-asof-key + lo, left-asof-key + hi] (inclusive).
  lo and hi are already in raw (possibly converted) units.
  Returns a dataset with all left columns plus one column per :agg entry."
  [left right left-keys right-keys lo hi agg-map]
  (when (nil? agg-map)
    (throw (ex-info ":how :window requires an :agg map"
                    {:dt/error :join-missing-agg})))
  (let [pairs (vec (asof/window-indices left right left-keys right-keys lo hi))
        agg-keys (vec (keys agg-map))
        compiled-fns (mapv compile-agg-fn (vals agg-map))
        sub-datasets (mapv (fn [[_li matched-idxs]]
                             (ds/select-rows right matched-idxs))
                           pairs)]
    (reduce (fn [d [col-kw compiled-fn]]
              (ds/add-column
               d
               (ds/new-column col-kw (mapv compiled-fn sub-datasets))))
            left
            (map vector agg-keys compiled-fns))))

;;; ---- Main join function ----------------------------------------------------

(defn join
  "Join two datasets. Returns a dataset.

  Options (keyword args):
    :on        — column keyword or vector of keywords (same name in both datasets)
    :left-on   — column keyword(s) for left dataset (use with :right-on)
    :right-on  — column keyword(s) for right dataset (use with :left-on)
    :how       — join type: :inner (default), :left, :right, :outer, :asof, :window
    :validate  — cardinality check: :1:1, :1:m, :m:1, :m:m (not for :window)
    :report    — if true, print merge diagnostics (not for :window)
    :direction — (asof only) :backward (default), :forward, or :nearest.
                 :backward = last right where right-key <= left-key.
                 :forward  = first right where right-key >= left-key.
                 :nearest  = closest by abs distance; ties prefer :backward.
    :tolerance — (asof only) numeric max abs distance; matches exceeding it
                 produce nil. Requires a numeric asof key. nil = unbounded.
    :window    — (window only) window spec relative to each left row's asof-key.
                 Formats: [lo hi], [lo hi unit], or [lo unit hi].
                 E.g. [-5 0 :minutes] = 5-minute lookback (inclusive).
                 Units: :seconds, :minutes, :hours, :days, :weeks.
    :agg       — (window only) map of {output-col agg-fn}. agg-fn can be a
                 #dt/e expression or a plain fn receiving the matched sub-dataset.
                 Empty windows return nil for #dt/e exprs; plain fns receive a
                 0-row sub-dataset and return their natural result (e.g. nrow -> 0).

  Must provide either :on or both :left-on and :right-on.

  As-of join (:how :asof):
    The last column in :on (or :left-on/:right-on) is the asof column;
    preceding columns are exact-match keys. All left rows are preserved;
    unmatched or out-of-tolerance rows get nil for right columns.

  Window join (:how :window):
    The last column in :on is the asof column; preceding columns are exact-match
    keys. For each left row, ALL right rows within the window are collected and
    aggregated via :agg. All left rows are preserved; empty windows produce nil
    (or 0 for plain-fn count aggs) for each agg column."
  [left right & {:keys [on left-on right-on how validate report direction tolerance window agg]
                 :or {how :inner report false direction :backward}}]
  (let [how-kw (if (string? how) (keyword how) how)
        left-keys (or (normalize-keys on) (normalize-keys left-on))
        right-keys (or (normalize-keys on) (normalize-keys right-on))]

    ;; --- shared validation ---
    (when (and on (or left-on right-on))
      (throw (ex-info "Cannot combine :on with :left-on/:right-on"
                      {:dt/error :join-invalid-keys})))
    (when (and (not on) (not (and left-on right-on)))
      (throw (ex-info "Must provide either :on or both :left-on and :right-on"
                      {:dt/error :join-missing-keys})))
    (when-not (#{:inner :left :right :outer :asof :window} how-kw)
      (throw (ex-info (str "Unknown join type: " how-kw
                           ". Must be :inner, :left, :right, :outer, :asof, or :window.")
                      {:dt/error :join-unknown-how :dt/how how-kw})))

    (cond
      ;; --- :asof dispatch ---
      (= how-kw :asof)
      (do
        (when-not (#{:backward :forward :nearest} direction)
          (throw (ex-info (str "Unknown :direction: " direction
                               ". Must be :backward, :forward, or :nearest.")
                          {:dt/error :join-unknown-direction :dt/direction direction})))
        (check-tolerance-compatible! tolerance left right left-keys right-keys)
        (when validate
          (when-not (#{:1:1 :1:m :m:1 :m:m} validate)
            (throw (ex-info (str "Unknown :validate value: " validate
                                 ". Must be :1:1, :1:m, :m:1, or :m:m.")
                            {:dt/error :join-unknown-validate :dt/validate validate})))
          (when (and (#{:1:1 :m:1} validate) (has-duplicate-keys? right right-keys))
            (throw (ex-info (str "Cardinality violation: right dataset has duplicate keys "
                                 "(expected " validate ", right side must be unique).")
                            {:dt/error :join-cardinality-violation
                             :dt/validate validate
                             :dt/side :right
                             :dt/keys right-keys}))))
        (when report
          (print-report left right left-keys right-keys))
        (let [pairs (asof/asof-match left right left-keys right-keys direction tolerance)]
          (asof/build-result left right pairs right-keys)))

      ;; --- :window dispatch ---
      (= how-kw :window)
      (let [{:keys [lo hi]} (parse-window-spec window)]
        (apply-window-join left right left-keys right-keys lo hi agg))

      ;; --- regular join dispatch ---
      :else
      (let [merge-opts (cond-> {:how how-kw}
                         on (assoc :on (normalize-keys on))
                         left-on (assoc :left-on (normalize-keys left-on))
                         right-on (assoc :right-on (normalize-keys right-on)))]
        (when validate
          (when-not (#{:1:1 :1:m :m:1 :m:m} validate)
            (throw (ex-info (str "Unknown :validate value: " validate
                                 ". Must be :1:1, :1:m, :m:1, or :m:m.")
                            {:dt/error :join-unknown-validate :dt/validate validate})))
          (when (and (#{:1:1 :1:m} validate) (has-duplicate-keys? left left-keys))
            (throw (ex-info (str "Cardinality violation: left dataset has duplicate keys "
                                 "(expected " validate ", left side must be unique).")
                            {:dt/error :join-cardinality-violation
                             :dt/validate validate
                             :dt/side :left
                             :dt/keys left-keys})))
          (when (and (#{:1:1 :m:1} validate) (has-duplicate-keys? right right-keys))
            (throw (ex-info (str "Cardinality violation: right dataset has duplicate keys "
                                 "(expected " validate ", right side must be unique).")
                            {:dt/error :join-cardinality-violation
                             :dt/validate validate
                             :dt/side :right
                             :dt/keys right-keys}))))
        (when report
          (print-report left right left-keys right-keys))
        (ds-join/pd-merge left right merge-opts)))))
