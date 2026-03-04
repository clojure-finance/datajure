(ns datajure.clay
  "Rich Clay/Kindly notebook viewers for Datajure datasets and expressions.

  Clay integration uses the Kindly convention — values are annotated with
  :kind/hiccup metadata so any Kindly-compatible tool (Clay, Portal, etc.)
  renders them as rich HTML.

  Two usage modes:

  1. Explicit wrapping (always works, no install step):

     (dc/view ds)                         ;; rich dataset table
     (dc/view-expr #dt/e (/ :mass 1000))  ;; expression display
     (dc/view-describe (du/describe ds))   ;; enhanced describe

  2. Auto-rendering via install! (registers Kindly advisor):

     (dc/install!)
     ;; Now all datasets and #dt/e exprs auto-render in Clay notebooks

  Usage in a Clay notebook:

    (ns my-notebook
      (:require [datajure.clay :as dc]
                [datajure.core :as core]
                [scicloj.clay.v2.api :as clay]))
    (dc/install!)

    (core/dt ds :by [:species] :agg {:n core/N})"
  (:require [datajure.expr :as expr]
            [datajure.clerk :as clerk-views]
            [tech.v3.dataset :as ds])
  (:import [tech.v3.dataset.impl.dataset Dataset]))

(defn- dataset? [x]
  (instance? Dataset x))

(defn- describe-ds? [x]
  (and (dataset? x)
       (some #{:column} (ds/column-names x))
       (some #{:n} (ds/column-names x))
       (some #{:mean} (ds/column-names x))))

(defn- kindly-hiccup
  "Wrap a Hiccup form with Kindly :kind/hiccup metadata."
  [hiccup]
  (vary-meta hiccup assoc :kindly/kind :kind/hiccup))

(defn view
  "Render a dataset as a rich HTML table via Kindly.
  Returns a Hiccup value annotated with :kind/hiccup metadata.
  Options: :max-rows, :max-cols (same as clerk/dataset->hiccup)."
  ([dataset] (view dataset {}))
  ([dataset opts]
   (kindly-hiccup (clerk-views/dataset->hiccup dataset opts))))

(defn view-expr
  "Render a #dt/e expression AST as rich HTML via Kindly.
  Returns a Hiccup value annotated with :kind/hiccup metadata."
  [node]
  (kindly-hiccup (clerk-views/expr->hiccup node)))

(defn view-describe
  "Render a du/describe result as an enhanced HTML table via Kindly.
  Returns a Hiccup value annotated with :kind/hiccup metadata."
  [desc-ds]
  (kindly-hiccup (clerk-views/describe->hiccup desc-ds)))

(defn datajure-advisor
  "Kindly advisor that assigns :kind/hiccup to Datajure types.
  Returns a vector of [kind] pairs when the value is recognized,
  nil otherwise. Checks in order: describe output, dataset, #dt/e expr."
  [{:keys [value]}]
  (cond
    (describe-ds? value) [[:kind/hiccup]]
    (dataset? value) [[:kind/hiccup]]
    (expr/expr-node? value) [[:kind/hiccup]]
    :else nil))

(defn- transform-value
  "Transform a Datajure value to its rich Hiccup representation.
  Called by the Kindly advisor pipeline when our advisor matches."
  [value]
  (cond
    (describe-ds? value) (clerk-views/describe->hiccup value)
    (dataset? value) (clerk-views/dataset->hiccup value)
    (expr/expr-node? value) (clerk-views/expr->hiccup value)
    :else value))

(defn install!
  "Register Datajure custom rendering with Clay/Kindly.

  This registers a Kindly advisor that automatically renders:
  - tech.v3.dataset datasets as rich HTML tables with column types
  - #dt/e AST nodes as readable expressions with metadata
  - du/describe output with conditional formatting

  Call at the top of your Clay notebook after requiring this namespace."
  []
  (let [set-advisors! (requiring-resolve 'scicloj.kindly-advice.v1.api/set-advisors!)
        default-advisors @(requiring-resolve 'scicloj.kindly-advice.v1.api/default-advisors)
        advisor (fn [{:as context :keys [value]}]
                  (cond
                    (describe-ds? value)
                    (assoc context
                           :kind :kind/hiccup
                           :value (clerk-views/describe->hiccup value))
                    (dataset? value)
                    (assoc context
                           :kind :kind/hiccup
                           :value (clerk-views/dataset->hiccup value))
                    (expr/expr-node? value)
                    (assoc context
                           :kind :kind/hiccup
                           :value (clerk-views/expr->hiccup value))
                    :else context))]
    (set-advisors! (cons advisor default-advisors))
    :installed))
