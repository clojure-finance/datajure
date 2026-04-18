(ns datajure.join
  "Join functions for datajure. Wraps tech.v3.dataset.join/pd-merge for
   regular joins and datajure.asof for as-of joins."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.join :as ds-join]
            [datajure.asof :as asof]))

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

(defn join
  "Join two datasets. Returns a dataset.

  Options (keyword args):
    :on        — column keyword or vector of keywords (same name in both datasets)
    :left-on   — column keyword(s) for left dataset (use with :right-on)
    :right-on  — column keyword(s) for right dataset (use with :left-on)
    :how       — join type: :inner (default), :left, :right, :outer, :asof
    :validate  — cardinality check: :1:1, :1:m, :m:1, :m:m
    :report    — if true, print merge diagnostics (matched/left-only/right-only)

  Must provide either :on or both :left-on and :right-on.

  As-of join (:how :asof):
    The last column in :on (or :left-on/:right-on) is the asof column —
    each left row is matched to the last right row where right-key <= left-key.
    Preceding columns are exact-match keys. All left rows are preserved;
    unmatched rows get nil for right columns."
  [left right & {:keys [on left-on right-on how validate report direction tolerance]
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
    (when-not (#{:inner :left :right :outer :asof} how-kw)
      (throw (ex-info (str "Unknown join type: " how-kw
                           ". Must be :inner, :left, :right, :outer, or :asof.")
                      {:dt/error :join-unknown-how :dt/how how-kw})))

    ;; --- :asof dispatch ---
    (if (= how-kw :asof)
      (do
        (when-not (#{:backward :forward :nearest} direction)
          (throw (ex-info (str "Unknown :direction: " direction
                               ". Must be :backward, :forward, or :nearest.")
                          {:dt/error :join-unknown-direction :dt/direction direction})))
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

      ;; --- regular join dispatch ---
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
