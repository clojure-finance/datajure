(ns datajure.core-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.core :as core]))

(def ^:private penguins
  (ds/->dataset {:species ["Adelie" "Adelie" "Gentoo" "Gentoo" "Chinstrap"]
                 :mass [3750 3800 5000 4800 3500]
                 :year [2007 2008 2007 2008 2007]}))

;; ---------------------------------------------------------------------------
;; Regression: boolean mask vs row-index interpretation in ds/select-rows
;;
;; Raw boolean-array is treated by ds/select-rows as row *indices* (false=0,
;; true=1), not a boolean mask. All boolean results from #dt/e expressions must
;; return a tech.v3 typed reader (dtype/make-reader :boolean ...) so that
;; select-rows interprets them correctly as masks.
;; ---------------------------------------------------------------------------

(deftest in-returns-correct-rows
  (testing ":in selects matching rows, not row indices"
    (let [result (core/dt penguins :where #dt/e (in :species #{"Gentoo" "Chinstrap"}))]
      (is (= 3 (ds/row-count result)))
      (is (= #{"Gentoo" "Chinstrap"} (set (vec (result :species))))))))

(deftest in-nil-set-returns-zero-rows
  (testing ":in with nil set returns 0 rows (nil-safety)"
    (let [result (core/dt penguins :where #dt/e (in :species nil))]
      (is (= 0 (ds/row-count result))))))

(deftest nil-safety-comparison-returns-zero-rows
  (testing "comparison with nil literal returns 0 rows, not all rows"
    (let [result (core/dt penguins :where #dt/e (> :mass nil))]
      (is (= 0 (ds/row-count result))))))

(deftest between-nil-bound-returns-zero-rows
  (testing ":between? with nil bound returns 0 rows (nil-safety)"
    (is (= 0 (ds/row-count (core/dt penguins :where #dt/e (between? :mass nil 5000)))))
    (is (= 0 (ds/row-count (core/dt penguins :where #dt/e (between? :mass 3700 nil)))))))

;; ---------------------------------------------------------------------------
;; :in and :between? correctness
;; ---------------------------------------------------------------------------

(deftest in-correctness
  (testing ":in matches expected rows"
    (let [result (core/dt penguins :where #dt/e (in :species #{"Adelie"}))]
      (is (= 2 (ds/row-count result)))
      (is (every? #{"Adelie"} (vec (result :species)))))))

(deftest between-correctness
  (testing ":between? is inclusive on both bounds"
    (let [result (core/dt penguins :where #dt/e (between? :mass 3750 4800))]
      (is (= 3 (ds/row-count result)))
      (is (= #{3750 3800 4800} (set (vec (result :mass))))))))

(deftest in-combined-with-and
  (testing ":in composes correctly with :and and :between?"
    (let [result (core/dt penguins
                          :where #dt/e (and (in :species #{"Gentoo" "Adelie"})
                                            (between? :mass 3700 4900)))]
      (is (= 3 (ds/row-count result)))
      (is (every? #{"Gentoo" "Adelie"} (vec (result :species)))))))

(deftest group-agg-basic
  (testing ":by + :agg produces correct group summaries"
    (let [result (core/dt penguins :by [:species] :agg {:n core/N :avg #dt/e (mn :mass)})]
      (is (= 3 (ds/row-count result)))
      (is (= #{:species :n :avg} (set (ds/column-names result))))
      (let [gentoo (first (ds/mapseq-reader (core/dt result :where #dt/e (= :species "Gentoo"))))]
        (is (= 2 (:n gentoo)))
        (is (= 4900.0 (:avg gentoo)))))))

(deftest group-agg-multi-column-by
  (testing ":by with multiple columns"
    (let [result (core/dt penguins :by [:species :year] :agg {:n core/N})]
      (is (= 5 (ds/row-count result)))
      (is (every? #(= 1 %) (vec (result :n)))))))

(deftest group-set-window-mode
  (testing ":by + :set preserves all rows and adds group-level column"
    (let [result (core/dt penguins :by [:species] :set {:mean-mass #dt/e (mn :mass)})]
      (is (= 5 (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :mean-mass)))))

(deftest group-set-sequential-demean
  (testing ":by + :set sequential derivation (demean within group)"
    (let [result (core/dt penguins :by [:species]
                          :set [[:mean-mass #dt/e (mn :mass)]
                                [:diff #dt/e (- :mass :mean-mass)]])
          adelie-diffs (->> (ds/mapseq-reader result)
                            (filter #(= "Adelie" (:species %)))
                            (map :diff))]
      (is (= 5 (ds/row-count result)))
      (is (= -25.0 (first adelie-diffs)))
      (is (= 25.0 (second adelie-diffs))))))

(deftest order-by-desc
  (testing ":order-by descending sorts correctly"
    (let [result (core/dt penguins :order-by [(core/desc :mass)])]
      (is (= [5000 4800 3800 3750 3500] (vec (result :mass)))))))

(deftest order-by-multi-key
  (testing ":order-by with multiple keys and bare keyword"
    (let [result (core/dt penguins :order-by [(core/asc :year) (core/desc :mass)])]
      (is (= [2007 2007 2007 2008 2008] (vec (result :year))))
      (is (= [5000 3750 3500 4800 3800] (vec (result :mass)))))))

(deftest select-vector
  (testing ":select with vector of keywords"
    (is (= [:species :mass] (vec (ds/column-names (core/dt penguins :select [:species :mass])))))))

(deftest select-single-keyword
  (testing ":select with single keyword"
    (is (= [:species] (vec (ds/column-names (core/dt penguins :select :species)))))))

(deftest select-not
  (testing ":select with [:not ...] exclusion"
    (is (= #{:species :mass} (set (ds/column-names (core/dt penguins :select [:not :year])))))))

(deftest select-regex
  (testing ":select with regex"
    (is (= [:mass] (vec (ds/column-names (core/dt penguins :select #"mass.*")))))))

(deftest select-predicate
  (testing ":select with predicate fn"
    (is (= #{:species :mass} (set (ds/column-names (core/dt penguins :select #(not= % :year))))))))

(deftest select-rename-map
  (testing ":select with map for rename-on-select"
    (is (= #{:sp :m} (set (ds/column-names (core/dt penguins :select {:species :sp :mass :m})))))))

(deftest plain-fn-where
  (testing "plain fn predicate in :where"
    (let [result (core/dt penguins :where #(> (:mass %) 4000))]
      (is (= 2 (ds/row-count result)))
      (is (every? #(> % 4000) (vec (result :mass)))))))

(deftest plain-fn-set
  (testing "plain fn in :set derives column from row map"
    (let [result (core/dt penguins :set {:mass-kg #(/ (:mass %) 1000.0)})]
      (is (= 5 (ds/row-count result)))
      (is (= 3.75 (first (vec (result :mass-kg))))))))

(deftest plain-fn-agg
  (testing "plain fn in :agg receives group dataset"
    (let [result (core/dt penguins :by [:species]
                          :agg {:avg #(tech.v3.datatype.functional/mean (:mass %))})]
      (is (= 3 (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :avg)))))

(deftest set-agg-conflict
  (testing ":set + :agg in same call throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Cannot combine"
                          (core/dt penguins :set {:x #dt/e (/ :mass 1000)} :agg {:n core/N})))))

(deftest map-set-simultaneous-semantics
  (testing "map :set evaluates all against original dataset (simultaneous)"
    (is (thrown? Exception
                 (core/dt penguins :set {:a #dt/e (/ :mass 1000)
                                         :b #dt/e (* :a 1000)})))))

(deftest vector-set-sequential-semantics
  (testing "vector-of-pairs :set allows referencing earlier columns"
    (let [result (core/dt penguins :set [[:a #dt/e (/ :mass 1000)]
                                         [:b #dt/e (* :a 2)]])]
      (is (= 5 (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :b)))))

(deftest whole-table-agg
  (testing "whole-table :agg without :by"
    (let [result (core/dt penguins :agg {:n core/N :total #dt/e (sm :mass)})]
      (is (= 1 (ds/row-count result)))
      (is (= 5 (first (vec (result :n)))))
      (is (= 20850.0 (first (vec (result :total))))))))

(deftest threading-pipeline
  (testing "threaded pipeline with HAVING equivalent"
    (let [result (-> penguins
                     (core/dt :by [:species] :agg {:n core/N :avg #dt/e (mn :mass)})
                     (core/dt :where #dt/e (>= :n 2))
                     (core/dt :order-by [(core/desc :avg)]))]
      (is (= 2 (ds/row-count result)))
      (is (= "Gentoo" (first (vec (result :species))))))))

(deftest unknown-column-validation
  (testing "missing column in #dt/e throws with :dt/error :unknown-column"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown column"
                          (core/dt penguins :where #dt/e (> :nonexistent 100))))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown column"
                          (core/dt penguins :set {:x #dt/e (+ :nonexistent 1)})))
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown column"
                          (core/dt penguins :agg {:avg #dt/e (mn :nonexistent)})))))
