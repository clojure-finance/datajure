(ns datajure.join
  "Join functions for datajure. Wraps tech.v3.dataset.join/pd-merge with
   keyword-driven syntax matching the datajure spec."
  (:require [tech.v3.dataset :as ds]
            [tech.v3.dataset.join :as ds-join]))

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

(defn join
  "Join two datasets. Returns a dataset.

  Options (keyword args):
    :on        — column keyword or vector of keywords (same name in both datasets)
    :left-on   — column keyword(s) for left dataset (use with :right-on)
    :right-on  — column keyword(s) for right dataset (use with :left-on)
    :how       — join type: :inner (default), :left, :right, :outer
    :validate  — cardinality check: :1:1, :1:m, :m:1, :m:m
    :report    — if true, print merge diagnostics (matched/left-only/right-only)

  Must provide either :on or both :left-on and :right-on."
  [left right & {:keys [on left-on right-on how validate report]
                 :or {how :inner report false}}]
  (let [how-kw (if (string? how) (keyword how) how)
        left-keys (or (some-> on (as-> o (if (keyword? o) [o] o)))
                      (some-> left-on (as-> o (if (keyword? o) [o] o))))
        right-keys (or (some-> on (as-> o (if (keyword? o) [o] o)))
                       (some-> right-on (as-> o (if (keyword? o) [o] o))))
        merge-opts (cond-> {:how how-kw}
                     on (assoc :on (if (keyword? on) [on] on))
                     left-on (assoc :left-on (if (keyword? left-on) [left-on] left-on))
                     right-on (assoc :right-on (if (keyword? right-on) [right-on] right-on)))]
    (when (and on (or left-on right-on))
      (throw (ex-info "Cannot combine :on with :left-on/:right-on"
                      {:dt/error :join-invalid-keys})))
    (when (and (not on) (not (and left-on right-on)))
      (throw (ex-info "Must provide either :on or both :left-on and :right-on"
                      {:dt/error :join-missing-keys})))
    (when-not (#{:inner :left :right :outer} how-kw)
      (throw (ex-info (str "Unknown join type: " how-kw ". Must be :inner, :left, :right, or :outer.")
                      {:dt/error :join-unknown-how :dt/how how-kw})))
    (when validate
      (when-not (#{:1:1 :1:m :m:1 :m:m} validate)
        (throw (ex-info (str "Unknown :validate value: " validate ". Must be :1:1, :1:m, :m:1, or :m:m.")
                        {:dt/error :join-unknown-validate :dt/validate validate})))
      (when (and (#{:1:1 :1:m} validate) (has-duplicate-keys? left left-keys))
        (throw (ex-info (str "Cardinality violation: left dataset has duplicate keys (expected "
                             validate ", left side must be unique).")
                        {:dt/error :join-cardinality-violation
                         :dt/validate validate
                         :dt/side :left
                         :dt/keys left-keys})))
      (when (and (#{:1:1 :m:1} validate) (has-duplicate-keys? right right-keys))
        (throw (ex-info (str "Cardinality violation: right dataset has duplicate keys (expected "
                             validate ", right side must be unique).")
                        {:dt/error :join-cardinality-violation
                         :dt/validate validate
                         :dt/side :right
                         :dt/keys right-keys}))))
    (when report
      (print-report left right left-keys right-keys))
    (ds-join/pd-merge left right merge-opts)))
