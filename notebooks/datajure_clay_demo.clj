;; # Datajure v2 — Clay Notebook Demo
;;
;; This notebook demonstrates Datajure's Clay/Kindly integration.
;; Rich rendering of datasets, expressions, and describe output.

(ns datajure-clay-demo
  (:require [datajure.core :as core]
            [datajure.clay :as dc]
            [datajure.util :as du]
            [datajure.expr :as expr]
            [tech.v3.dataset :as ds]
            [scicloj.kindly.v4.kind :as kind]))

;; ## Setup — register Datajure viewers
(dc/install!)

;; ## Sample Dataset

(def penguins
  (ds/->dataset {:species ["Adelie" "Adelie" "Gentoo" "Gentoo" "Chinstrap" "Chinstrap"
                           "Adelie" "Gentoo" "Chinstrap"]
                 :year [2007 2008 2007 2008 2007 2008 2009 2009 2009]
                 :mass [3750.0 3800.0 5200.0 5400.0 3800.0 3750.0 3900.0 5100.0 3700.0]
                 :height [0.39 0.40 0.46 0.47 0.41 0.39 0.41 0.45 0.40]
                 :flipper [181.0 186.0 217.0 230.0 195.0 192.0 190.0 215.0 193.0]}
                {:dataset-name "penguins"}))

;; Datasets auto-render as rich tables after install!
penguins

;; ## Descriptive Statistics
(du/describe penguins)

;; ## Filter and Select
(core/dt penguins
         :where #dt/e (> :mass 4000)
         :select [:species :mass :height])

;; ## Derive Columns
(core/dt penguins
         :set {:bmi #dt/e (/ :mass (sq :height))})

;; ## Group + Aggregate
(core/dt penguins
         :by [:species]
         :agg {:n core/N
               :avg-mass #dt/e (mn :mass)
               :avg-height #dt/e (mn :height)})

;; ## Window Functions
(core/dt penguins
         :by [:species]
         :within-order [(core/desc :mass)]
         :set {:rank #dt/e (win/rank :mass)})

;; ## Expression Display
;;
;; `#dt/e` expressions render with syntax highlighting and metadata:

#dt/e (/ :mass (sq :height))

;; ## Composed Expressions
(def bmi #dt/e (/ :mass (sq :height)))
(def high-bmi #dt/e (> bmi 25000))

high-bmi

;; ## Conditional Derivation
(core/dt penguins
         :set {:size #dt/e (cond
                             (> :mass 5000) "large"
                             (> :mass 3800) "medium"
                             :else "small")})

;; ## Full Pipeline
(-> penguins
    (core/dt :where #dt/e (> :year 2007)
             :by [:species]
             :agg {:n core/N :avg-mass #dt/e (mn :mass)})
    (core/dt :order-by [(core/desc :avg-mass)]))

;; ## Explicit Wrapping (alternative to install!)
;;
;; For fine-grained control, use `dc/view`, `dc/view-expr`, `dc/view-describe`:

(dc/view penguins {:max-rows 3})

(dc/view-expr #dt/e (and (> :mass 4000) (< :year 2009)))

(dc/view-describe (du/describe penguins [:mass :height]))
