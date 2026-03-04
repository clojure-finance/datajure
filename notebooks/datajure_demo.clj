;; # Datajure v2 — Notebook Demo
;;
;; This notebook demonstrates Datajure's rich Clerk integration.
;; Start Clerk with: `(clerk/serve! {:watch-paths ["notebooks"]})`

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(ns datajure-demo
  (:require [datajure.core :as core]
            [datajure.clerk :as dc]
            [datajure.util :as du]
            [datajure.expr :as expr]
            [tech.v3.dataset :as ds]
            [nextjournal.clerk :as clerk]))

^{:nextjournal.clerk/visibility {:code :hide :result :hide}}
(dc/install!)

;; ## Dataset Viewer
;;
;; Datasets render as rich tables with column types, missing value
;; indicators, and automatic truncation for large datasets.

(def penguins
  (ds/->dataset {:species ["Adelie" "Adelie" "Gentoo" "Gentoo" "Chinstrap" "Chinstrap"]
                 :mass [3750 3800 5000 4800 3500 3700]
                 :year [2007 2008 2007 2008 2007 2009]
                 :height [1.5 1.6 1.8 1.75 1.4 1.45]
                 :bill-len [39.1 39.5 47.3 nil 46.5 49.6]}))

;; ## Querying with `dt`
;;
;; All `dt` results are datasets and render automatically.

;; ### Filter
(core/dt penguins :where #dt/e (> :mass 4000))

;; ### Derive columns
(core/dt penguins :set {:bmi #dt/e (/ :mass (sq :height))})

;; ### Group + Aggregate
(core/dt penguins :by [:species] :agg {:n core/N :avg-mass #dt/e (mn :mass)})

;; ### Window functions
(core/dt penguins
         :by [:species]
         :within-order [(core/desc :mass)]
         :set {:rank #dt/e (win/rank :mass)})

;; ### Conditional derivation
(core/dt penguins :set {:size #dt/e (cond (> :mass 4500) "large"
                                          (> :mass 3600) "medium"
                                          :else "small")})

;; ### Nil handling
(core/dt penguins :set {:bill #dt/e (coalesce :bill-len 0.0)})

;; ## Expression Viewer
;;
;; `#dt/e` AST nodes render with syntax highlighting,
;; column references, and window function indicators.

#dt/e (/ :mass (sq :height))

#dt/e (cond (> :bmi 40) "severe"
            (> :bmi 30) "obese"
            (> :bmi 25) "overweight"
            :else "normal")

#dt/e (win/rank :mass)

;; ### Reusable expressions
(def bmi #dt/e (/ :mass (sq :height)))

#dt/e (mn bmi)

;; ## Describe Viewer
;;
;; `du/describe` output gets enhanced formatting with
;; highlighted missing value counts.

(du/describe penguins)

(du/describe penguins [:mass :height :bill-len])

;; ## Full Pipeline
;;
;; Threading multiple `dt` calls — each step renders automatically.

(-> penguins
    (core/dt :where #dt/e (> :year 2007)
             :by [:species]
             :agg {:n core/N :avg-mass #dt/e (mn :mass)})
    (core/dt :order-by [(core/desc :avg-mass)]))
