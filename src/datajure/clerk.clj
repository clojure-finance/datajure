(ns datajure.clerk
  "Rich Clerk notebook viewers for Datajure datasets and expressions.

  Usage — require and call install! at the top of your notebook:

    (ns my-notebook
      (:require [datajure.clerk :as dc]
                [datajure.core :as core]
                [nextjournal.clerk :as clerk]))
    (dc/install!)

  This registers custom viewers that automatically render:
  - tech.v3.dataset datasets as rich HTML tables with column types
  - #dt/e AST nodes as readable expressions
  - du/describe output with conditional formatting"
  (:require [datajure.expr :as expr]
            [tech.v3.dataset :as ds]
            [tech.v3.datatype :as dtype]
            [tech.v3.datatype.casting :as casting])
  (:import [tech.v3.dataset.impl.dataset Dataset]))

(def ^:private max-display-rows 25)
(def ^:private max-display-cols 30)

(defn- dataset? [x]
  (instance? Dataset x))

(defn- column-info [dataset col-name]
  (let [col (dataset col-name)
        dt (-> col meta :datatype)
        missing (.missing col)]
    {:name col-name
     :datatype dt
     :n-missing (dtype/ecount missing)
     :numeric? (casting/numeric-type? dt)}))

(defn- format-val [v]
  (cond
    (nil? v) "nil"
    (float? v) (format "%.4g" (double v))
    :else (str v)))

(defn- dtype-badge-color [dt]
  (cond
    (casting/numeric-type? dt) "#e8f5e9"
    (= :string dt) "#e3f2fd"
    (= :boolean dt) "#fff3e0"
    :else "#f5f5f5"))

(defn- dtype-text-color [dt]
  (cond
    (casting/numeric-type? dt) "#2e7d32"
    (= :string dt) "#1565c0"
    (= :boolean dt) "#e65100"
    :else "#616161"))

(defn dataset->hiccup
  "Convert a dataset to a rich Hiccup table representation.
  Options:
    :max-rows  — max rows to display (default 25)
    :max-cols  — max columns to display (default 30)"
  ([dataset] (dataset->hiccup dataset {}))
  ([dataset {:keys [max-rows max-cols]
             :or {max-rows max-display-rows
                  max-cols max-display-cols}}]
   (let [col-names (vec (ds/column-names dataset))
         n-rows (ds/row-count dataset)
         n-cols (ds/column-count dataset)
         show-cols (if (> n-cols max-cols) (subvec col-names 0 max-cols) col-names)
         cols-truncated? (> n-cols max-cols)
         show-rows (min n-rows max-rows)
         rows-truncated? (> n-rows max-rows)
         col-infos (mapv #(column-info dataset %) show-cols)
         ds-name (ds/dataset-name dataset)
         rows (ds/rows (ds/select-rows dataset (range show-rows)) :as-maps)]
     [:div {:style {:font-family "'Inter', 'Segoe UI', system-ui, sans-serif"
                    :font-size "13px"
                    :max-width "100%"
                    :overflow-x "auto"}}
      ;; Header bar
      [:div {:style {:display "flex"
                     :align-items "center"
                     :gap "12px"
                     :padding "8px 12px"
                     :background "linear-gradient(135deg, #667eea 0%, #764ba2 100%)"
                     :color "white"
                     :border-radius "8px 8px 0 0"
                     :font-size "12px"}}
       [:span {:style {:font-weight "600" :font-size "14px"}}
        (if (= "_unnamed" ds-name) "Dataset" ds-name)]
       [:span {:style {:opacity "0.85"}}
        (str n-rows " rows × " n-cols " cols")]]
      ;; Table
      [:table {:style {:width "100%"
                       :border-collapse "collapse"
                       :border "1px solid #e0e0e0"}}
       ;; Column names
       [:thead
        [:tr {:style {:background "#fafafa"}}
         (for [{:keys [name]} col-infos]
           [:th {:key (str name)
                 :style {:padding "8px 12px"
                         :text-align "left"
                         :font-weight "600"
                         :border-bottom "2px solid #e0e0e0"
                         :white-space "nowrap"}}
            (clojure.core/name name)])
         (when cols-truncated?
           [:th {:style {:padding "8px 12px"
                         :text-align "center"
                         :color "#999"
                         :border-bottom "2px solid #e0e0e0"}}
            "…"])]
        ;; Type badges row
        [:tr {:style {:background "#fafafa"}}
         (for [{:keys [name datatype n-missing]} col-infos]
           [:td {:key (str "type-" name)
                 :style {:padding "2px 12px 6px"
                         :border-bottom "1px solid #e0e0e0"}}
            [:span {:style {:display "inline-block"
                            :padding "1px 6px"
                            :border-radius "3px"
                            :font-size "10px"
                            :font-weight "500"
                            :background (dtype-badge-color datatype)
                            :color (dtype-text-color datatype)}}
             (clojure.core/name datatype)]
            (when (pos? n-missing)
              [:span {:style {:margin-left "4px"
                              :font-size "10px"
                              :color "#ef5350"}}
               (str n-missing " nil")])])
         (when cols-truncated?
           [:td {:style {:padding "2px 12px 6px"
                         :border-bottom "1px solid #e0e0e0"}}])]]
       ;; Data rows
       [:tbody
        (for [[idx row] (map-indexed vector rows)]
          [:tr {:key idx
                :style {:background (if (even? idx) "white" "#fafafa")}}
           (for [{:keys [name datatype]} col-infos]
             (let [v (get row name)]
               [:td {:key (str idx "-" name)
                     :style (merge {:padding "6px 12px"
                                    :border-bottom "1px solid #f0f0f0"
                                    :white-space "nowrap"}
                                   (when (nil? v)
                                     {:color "#bbb" :font-style "italic"})
                                   (when (casting/numeric-type? datatype)
                                     {:text-align "right"
                                      :font-family "'JetBrains Mono', 'Fira Code', monospace"}))}
                (format-val v)]))
           (when cols-truncated?
             [:td {:style {:padding "6px 12px"
                           :text-align "center"
                           :color "#999"
                           :border-bottom "1px solid #f0f0f0"}}
              "…"])])
        ;; Truncation indicator
        (when rows-truncated?
          [:tr
           [:td {:col-span (+ (count col-infos) (if cols-truncated? 1 0))
                 :style {:padding "8px 12px"
                         :text-align "center"
                         :color "#999"
                         :font-style "italic"
                         :background "#fafafa"
                         :border-top "1px dashed #ddd"}}
            (str "… " (- n-rows max-rows) " more rows")]])]]])))

(def ^:private op-display
  {:+ "+" :- "-" :* "*" :div "/"
   :> ">" :< "<" :>= ">=" :<= "<=" := "="
   :and "and" :or "or" :not "not"
   :sq "sq" :log "log"
   :mn "mean" :sm "sum" :md "median" :sd "stddev"
   :nuniq "count-distinct"
   :in "in" :between? "between?"})

(def ^:private win-op-display
  {:win/rank "win/rank" :win/dense-rank "win/dense-rank"
   :win/row-number "win/row-number"
   :win/lag "win/lag" :win/lead "win/lead"
   :win/cumsum "win/cumsum" :win/cummin "win/cummin"
   :win/cummax "win/cummax" :win/cummean "win/cummean"
   :win/rleid "win/rleid"})

(def ^:private row-op-display
  {:row/sum "row/sum" :row/mean "row/mean"
   :row/min "row/min" :row/max "row/max"
   :row/count-nil "row/count-nil" :row/any-nil? "row/any-nil?"})

(defn ast->string
  "Convert a #dt/e AST node to a readable string representation."
  [node]
  (case (:node/type node)
    :col (str (:col/name node))
    :lit (let [v (:lit/value node)]
           (if (expr/expr-node? v)
             (ast->string v)
             (pr-str v)))
    :op (let [op-name (get op-display (:op/name node) (name (:op/name node)))
              args (map ast->string (:op/args node))]
          (str "(" op-name " " (clojure.string/join " " args) ")"))
    :win (let [op-name (get win-op-display (:win/op node) (name (:win/op node)))
               args (map ast->string (:win/args node))]
           (str "(" op-name " " (clojure.string/join " " args) ")"))
    :row (let [op-name (get row-op-display (:row/op node) (name (:row/op node)))
               args (map ast->string (:row/args node))]
           (str "(" op-name " " (clojure.string/join " " args) ")"))
    :if (letfn [(cond-chain? [n]
                  (and (= :if (:node/type n))
                       (let [e (:if/else n)]
                         (or (nil? e)
                             (= {:node/type :lit :lit/value nil} e)
                             (and (= :lit (-> e :node/type))
                                  (true? (-> e :lit/value)))
                             (= :if (:node/type e))))))
                (collect-branches [n]
                  (if (and (= :if (:node/type n))
                           (= :lit (-> n :if/pred :node/type))
                           (true? (-> n :if/pred :lit/value)))
                    [":else" (ast->string (:if/then n))]
                    (let [pair [(ast->string (:if/pred n))
                                (ast->string (:if/then n))]
                          e (:if/else n)]
                      (if (and e (= :if (:node/type e)))
                        (into pair (collect-branches e))
                        pair))))]
          (if (cond-chain? node)
            (str "(cond " (clojure.string/join " " (collect-branches node)) ")")
            (let [pred (ast->string (:if/pred node))
                  then (ast->string (:if/then node))
                  else-node (:if/else node)]
              (if (and else-node (not= {:node/type :lit :lit/value nil} else-node))
                (str "(if " pred " " then " " (ast->string else-node) ")")
                (str "(if " pred " " then ")")))))
    :let (let [bindings (:let/bindings node)
               body (ast->string (:let/body node))
               bind-strs (map #(str (name (:binding/name %)) " " (ast->string (:binding/expr %)))
                              bindings)]
           (str "(let [" (clojure.string/join " " bind-strs) "] " body ")"))
    :scan (str "(win/scan " (name (:scan/op node)) " " (ast->string (:scan/arg node)) ")")
    :each-prior (str "(win/each-prior " (name (:each-prior/op node)) " " (ast->string (:each-prior/arg node)) ")")
    :cut (let [base (str "(cut " (ast->string (:cut/col node)) " " (ast->string (:cut/n node)))]
           (if-let [from (:cut/from node)]
             (str base " :from " (ast->string from) ")")
             (str base ")")))
    :xbar (let [col (ast->string (:xbar/col node))
                width (ast->string (:xbar/width node))
                unit (:xbar/unit node)]
            (if unit
              (str "(xbar " col " " width " " unit ")")
              (str "(xbar " col " " width ")")))
    :binding-ref (name (:binding-ref/name node))
    :coalesce (str "(coalesce " (clojure.string/join " " (map ast->string (:coalesce/args node))) ")")
    (pr-str node)))

(defn expr->hiccup
  "Convert a #dt/e AST node to a rich Hiccup representation."
  [node]
  (let [expr-str (ast->string node)
        col-refs (expr/col-refs node)
        win-refs (expr/win-refs node)]
    [:div {:style {:font-family "'Inter', 'Segoe UI', system-ui, sans-serif"
                   :font-size "13px"
                   :border "1px solid #e0e0e0"
                   :border-radius "8px"
                   :overflow "hidden"}}
     ;; Header
     [:div {:style {:display "flex"
                    :align-items "center"
                    :gap "8px"
                    :padding "6px 12px"
                    :background "linear-gradient(135deg, #f093fb 0%, #f5576c 100%)"
                    :color "white"
                    :font-size "11px"
                    :font-weight "600"}}
      "#dt/e Expression"
      (when (seq win-refs)
        [:span {:style {:background "rgba(255,255,255,0.25)"
                        :padding "1px 6px"
                        :border-radius "3px"}}
         "window"])]
     ;; Expression
     [:div {:style {:padding "12px 16px"
                    :background "#1e1e1e"
                    :font-family "'JetBrains Mono', 'Fira Code', monospace"
                    :font-size "13px"
                    :color "#d4d4d4"}}
      expr-str]
     ;; Metadata footer
     [:div {:style {:padding "6px 12px"
                    :background "#fafafa"
                    :font-size "11px"
                    :color "#666"
                    :display "flex"
                    :gap "16px"}}
      [:span "Columns: "
       (if (seq col-refs)
         (clojure.string/join ", " (map name (sort col-refs)))
         "none")]
      (when (seq win-refs)
        [:span "Window ops: "
         (clojure.string/join ", " (map name (sort win-refs)))])]]))

(defn describe->hiccup
  "Convert a du/describe result dataset to an enhanced Hiccup table.
  Highlights missing data counts and formats statistics."
  [desc-ds]
  (let [col-names (vec (ds/column-names desc-ds))
        rows (ds/rows desc-ds :as-maps)
        n-rows (ds/row-count desc-ds)]
    [:div {:style {:font-family "'Inter', 'Segoe UI', system-ui, sans-serif"
                   :font-size "13px"
                   :max-width "100%"
                   :overflow-x "auto"}}
     ;; Header bar
     [:div {:style {:display "flex"
                    :align-items "center"
                    :gap "12px"
                    :padding "8px 12px"
                    :background "linear-gradient(135deg, #11998e 0%, #38ef7d 100%)"
                    :color "white"
                    :border-radius "8px 8px 0 0"
                    :font-size "12px"}}
      [:span {:style {:font-weight "600" :font-size "14px"}} "describe"]
      [:span {:style {:opacity "0.85"}} (str n-rows " columns")]]
     ;; Table
     [:table {:style {:width "100%"
                      :border-collapse "collapse"
                      :border "1px solid #e0e0e0"}}
      [:thead
       [:tr {:style {:background "#fafafa"}}
        (for [col-name col-names]
          [:th {:key (str col-name)
                :style {:padding "8px 12px"
                        :text-align (if (= col-name :column) "left" "right")
                        :font-weight "600"
                        :border-bottom "2px solid #e0e0e0"
                        :white-space "nowrap"}}
           (clojure.core/name col-name)])]]
      [:tbody
       (for [[idx row] (map-indexed vector rows)]
         [:tr {:key idx
               :style {:background (if (even? idx) "white" "#fafafa")}}
          (for [col-name col-names]
            (let [v (get row col-name)
                  is-missing-col? (= col-name :n-missing)
                  has-missing? (and is-missing-col? (number? v) (pos? v))]
              [:td {:key (str idx "-" col-name)
                    :style (merge {:padding "6px 12px"
                                   :border-bottom "1px solid #f0f0f0"
                                   :white-space "nowrap"}
                                  (when (not= col-name :column)
                                    {:text-align "right"
                                     :font-family "'JetBrains Mono', 'Fira Code', monospace"})
                                  (when (nil? v)
                                    {:color "#ccc"})
                                  (when has-missing?
                                    {:color "#ef5350"
                                     :font-weight "600"}))}
               (cond
                 (nil? v) "—"
                 (and (number? v) (not (integer? v))) (format "%.4g" (double v))
                 :else (str v))]))])]]]))

(def dataset-viewer
  "Clerk viewer for tech.v3.dataset datasets."
  {:pred dataset?
   :transform-fn (fn [wrapped-value]
                   (let [clerk (requiring-resolve 'nextjournal.clerk/html)]
                     (clerk (dataset->hiccup (:nextjournal/value wrapped-value)))))})

(def expr-viewer
  "Clerk viewer for #dt/e expression AST nodes."
  {:pred expr/expr-node?
   :transform-fn (fn [wrapped-value]
                   (let [clerk (requiring-resolve 'nextjournal.clerk/html)]
                     (clerk (expr->hiccup (:nextjournal/value wrapped-value)))))})

(def describe-viewer
  "Clerk viewer for du/describe output (datasets with :column as first col)."
  {:pred (fn [x]
           (and (dataset? x)
                (some #{:column} (ds/column-names x))
                (some #{:n} (ds/column-names x))
                (some #{:mean} (ds/column-names x))))
   :transform-fn (fn [wrapped-value]
                   (let [clerk (requiring-resolve 'nextjournal.clerk/html)]
                     (clerk (describe->hiccup (:nextjournal/value wrapped-value)))))})

(defn install!
  "Register Datajure custom viewers with Clerk.
  Call at the top of your notebook after requiring this namespace.

  The describe viewer is registered before the dataset viewer so that
  du/describe output gets enhanced formatting."
  []
  (let [add-viewers! (requiring-resolve 'nextjournal.clerk/add-viewers!)]
    (add-viewers! [describe-viewer expr-viewer dataset-viewer])))
