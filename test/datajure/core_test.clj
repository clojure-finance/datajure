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
        (is (= 4900.0 (:avg gentoo))))))
  (testing ":by with computed fn produces correct group summaries"
    (let [result (core/dt penguins
                          :by (fn [row] {:heavy? (> (:mass row) 4000)})
                          :agg {:n core/N})]
      (is (= 2 (ds/row-count result)))
      (is (= #{:heavy? :n} (set (ds/column-names result))))))
  (testing ":by with computed fn in window mode keeps all rows"
    (let [result (core/dt penguins
                          :by (fn [row] {:heavy? (> (:mass row) 4000)})
                          :set {:mean-mass #dt/e (mn :mass)})]
      (is (= (ds/row-count penguins) (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :mean-mass)))))

(deftest group-agg-multi-column-by
  (testing ":by with multiple columns"
    (let [result (core/dt penguins :by [:species :year] :agg {:n core/N})]
      (is (= 5 (ds/row-count result)))
      (is (every? #(= 1 %) (vec (result :n)))))))

(deftest group-set-window-mode
  (testing ":by + :set preserves all rows and adds group-level column"
    (let [result (core/dt penguins :by [:species] :set {:mean-mass #dt/e (mn :mass)})]
      (is (= 5 (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :mean-mass))))
  (testing ":within-order sorts rows within each partition before derivation"
    (let [ds (ds/->dataset {:species ["A" "A" "A" "B" "B"]
                            :mass [3800.0 3500.0 4000.0 5000.0 4600.0]})
          result (core/dt ds
                          :by [:species]
                          :within-order [(core/desc :mass)]
                          :set {:first-mass #dt/e (mn :mass)})]
      (is (= 5 (ds/row-count result)))
      (let [rows (ds/mapseq-reader result)
            a-rows (filter #(= "A" (:species %)) rows)
            masses (map :mass a-rows)]
        (is (= [4000.0 3800.0 3500.0] masses)))))
  (testing ":without-order produces same result as omitting :within-order"
    (let [ds (ds/->dataset {:species ["A" "A"] :mass [3800.0 3500.0]})
          r1 (core/dt ds :by [:species] :set {:x #dt/e (mn :mass)})
          r2 (core/dt ds :by [:species] :set {:x #dt/e (mn :mass)} :within-order [(core/asc :mass)])]
      (is (= (set (map :x (ds/mapseq-reader r1)))
             (set (map :x (ds/mapseq-reader r2))))))))

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
    (is (= #{:sp :m} (set (ds/column-names (core/dt penguins :select {:species :sp :mass :m}))))))
  (testing ":select :type/numerical keeps only numeric columns"
    (let [result (core/dt penguins :select :type/numerical)
          cols (set (ds/column-names result))]
      (is (contains? cols :mass))
      (is (contains? cols :year))
      (is (not (contains? cols :species)))))
  (testing ":select :!type/numerical keeps only non-numeric columns"
    (let [result (core/dt penguins :select :!type/numerical)
          cols (set (ds/column-names result))]
      (is (contains? cols :species))
      (is (not (contains? cols :mass)))
      (is (not (contains? cols :year))))))

(deftest select-unknown-column-errors
  (testing ":select with single unknown keyword throws structured error"
    (let [e (try (core/dt penguins :select :maas) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :unknown-column (:dt/error (ex-data e))))
      (is (= :select (:dt/context (ex-data e))))
      (is (= #{:maas} (:dt/columns (ex-data e))))
      (is (= [:mass] (get (:dt/closest (ex-data e)) :maas)))))
  (testing ":select with vector containing unknown column"
    (let [e (try (core/dt penguins :select [:species :maas]) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :unknown-column (:dt/error (ex-data e))))
      (is (= #{:maas} (:dt/columns (ex-data e))))))
  (testing ":select with map containing unknown column"
    (let [e (try (core/dt penguins :select {:maas :weight}) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :unknown-column (:dt/error (ex-data e))))
      (is (= #{:maas} (:dt/columns (ex-data e))))))
  (testing ":select with [:not ...] containing unknown column"
    (let [e (try (core/dt penguins :select [:not :maas]) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :unknown-column (:dt/error (ex-data e))))
      (is (= #{:maas} (:dt/columns (ex-data e)))))))

(deftest select-between
  (testing ":select with between — basic forward range"
    (let [ds (ds/->dataset {:a [1] :b [2] :c [3] :d [4] :e [5]})]
      (is (= [:b :c :d] (vec (ds/column-names (core/dt ds :select (core/between :b :d))))))))
  (testing ":select with between — full range"
    (let [ds (ds/->dataset {:a [1] :b [2] :c [3]})]
      (is (= [:a :b :c] (vec (ds/column-names (core/dt ds :select (core/between :a :c))))))))
  (testing ":select with between — single column (start = end)"
    (let [ds (ds/->dataset {:a [1] :b [2] :c [3]})]
      (is (= [:b] (vec (ds/column-names (core/dt ds :select (core/between :b :b))))))))
  (testing ":select with between — reversed endpoints selects same columns"
    (let [ds (ds/->dataset {:a [1] :b [2] :c [3] :d [4]})]
      (is (= [:b :c] (vec (ds/column-names (core/dt ds :select (core/between :c :b))))))))
  (testing ":select with between — unknown start column throws"
    (let [ds (ds/->dataset {:a [1] :b [2]})
          e (try (core/dt ds :select (core/between :z :b)) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :unknown-column (:dt/error (ex-data e))))))
  (testing ":select with between — unknown end column throws"
    (let [ds (ds/->dataset {:a [1] :b [2]})
          e (try (core/dt ds :select (core/between :a :z)) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :unknown-column (:dt/error (ex-data e)))))))

(deftest select-between-pipeline
  (testing ":select with between composes with :where and preserves row data"
    (let [d (ds/->dataset {:id [1 2 3] :a [10 20 30] :b [40 50 60] :c [70 80 90]})
          result (core/dt d :where #dt/e (> :a 10) :select (core/between :a :b))]
      (is (= [:a :b] (vec (ds/column-names result))))
      (is (= 2 (ds/row-count result)))
      (is (= [20 30] (vec (result :a)))))))

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
                          (core/dt penguins :set {:x #dt/e (/ :mass 1000)} :agg {:n core/N}))))
  (testing ":within-order with neither :set nor :agg throws"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #":within-order requires :set or :agg"
                          (core/dt penguins :within-order [(core/desc :mass)]))))
  (testing ":within-order with :agg is accepted (new in 2.0.6 — order-sensitive aggregation)"
    (core/reset-notes!)
    (is (ds/dataset? (core/dt penguins :by [:species] :agg {:n core/N}
                              :within-order [(core/desc :mass)]))))
  (testing ":within-order with :set (no :by) is accepted"
    (core/reset-notes!)
    (is (ds/dataset? (core/dt penguins :set {:x #dt/e (+ :mass 1)} :within-order [(core/desc :mass)]))))
  (testing ":within-order with :by + :set is accepted"
    (core/reset-notes!)
    (is (ds/dataset? (core/dt penguins :by [:species] :set {:x #dt/e (+ :mass 1)} :within-order [(core/desc :mass)])))))

(deftest map-set-simultaneous-semantics
  (testing "map :set cross-reference throws :map-set-cross-reference error"
    (let [e (try (core/dt penguins :set {:mass-k #dt/e (/ :mass 1000)
                                         :mass-2k #dt/e (* :mass-k 2)})
                 nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :map-set-cross-reference (-> e ex-data :dt/error)))
      (is (= :mass-2k (-> e ex-data :dt/column)))
      (is (= #{:mass-k} (-> e ex-data :dt/sibling-refs)))))
  (testing "map :set with independent expressions succeeds"
    (let [result (core/dt penguins :set {:mass-k #dt/e (/ :mass 1000)
                                         :yr2 #dt/e (* :year 2)})]
      (is (contains? (set (ds/column-names result)) :mass-k))
      (is (contains? (set (ds/column-names result)) :yr2)))))

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
                          (core/dt penguins :agg {:avg #dt/e (mn :nonexistent)}))))
  (testing "typo in column name suggests closest match"
    (let [err (try (core/dt penguins :where #dt/e (> :maas 4000))
                   nil
                   (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :unknown-column (:dt/error err)))
      (is (= [:mass] (get-in err [:dt/closest :maas])))))
  (testing "distant typo produces no suggestion"
    (let [err (try (core/dt penguins :where #dt/e (> :zzzzz 4000))
                   nil
                   (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (nil? (get-in err [:dt/closest :zzzzz]))))))

(deftest if-special-form
  (testing "#dt/e if — basic conditional derivation"
    (let [result (core/dt penguins :set {:heavy #dt/e (if (> :mass 4000) "heavy" "light")})]
      (is (= ["light" "light" "heavy" "heavy" "light"]
             (vec (result :heavy))))))
  (testing "#dt/e if — no else branch yields nil"
    (let [result (core/dt penguins :set {:big #dt/e (if (> :mass 4500) "big")})]
      (is (= ["big" "big"] (filterv some? (vec (result :big)))))))
  (testing "#dt/e if — used in :where"
    (let [result (core/dt penguins :where #dt/e (if (> :mass 4000) true false))]
      (is (= 2 (ds/row-count result))))))

(deftest cond-special-form
  (testing "#dt/e cond — multi-branch categorization"
    (let [result (core/dt penguins :set {:size #dt/e (cond
                                                       (> :mass 4900) "large"
                                                       (> :mass 3800) "medium"
                                                       :else "small")})]
      (is (= ["small" "small" "large" "medium" "small"]
             (vec (result :size))))))
  (testing "#dt/e cond — :else always fires as final fallback"
    (let [result (core/dt penguins :set {:x #dt/e (cond :else "always")})]
      (is (every? #(= "always" %) (vec (result :x)))))))

(deftest let-special-form
  (testing "#dt/e let — intermediate bindings not added as columns"
    (let [result (core/dt penguins :set {:norm #dt/e (let [m (/ :mass 1000.0)]
                                                       (* m m))})]
      (is (not (contains? (set (ds/column-names result)) :m)))
      (is (= 4 (count (ds/column-names result))))))
  (testing "#dt/e let — sequential bindings (later refs earlier)"
    (let [result (core/dt penguins
                          :set {:final #dt/e (let [a (/ :mass 1000.0)
                                                   b (* a 2.0)]
                                               b)})]
      (is (= [7.5 7.6 10.0 9.6 7.0]
             (mapv #(/ (Math/round (* % 10.0)) 10.0)
                   (vec (result :final)))))))
  (testing "#dt/e let — combined with if"
    (let [result (core/dt penguins
                          :set {:adj #dt/e (let [base (if (> :mass 4000) 1.1 1.0)]
                                             (* base :mass))})]
      (is (= [3750.0 3800.0 5500.0 5280.0 3500.0]
             (mapv double (vec (result :adj))))))))

(deftest coalesce-special-form
  (testing "coalesce — returns first non-nil column value"
    (let [ds (ds/->dataset {:a [nil nil 3.0] :b [nil 2.0 4.0]})
          result (core/dt ds :set {:c #dt/e (coalesce :a :b 0.0)})]
      (is (= [0.0 2.0 3.0] (vec (result :c))))))
  (testing "coalesce — literal fallback when all columns nil"
    (let [ds (ds/->dataset {:a [nil "Alice" nil] :b ["Unknown" "Bob" nil]})
          result (core/dt ds :set {:name #dt/e (coalesce :a :b "Default")})]
      (is (= ["Unknown" "Alice" "Default"] (vec (result :name))))))
  (testing "coalesce — single arg passthrough"
    (let [ds (ds/->dataset {:a [1.0 nil 3.0]})
          result (core/dt ds :set {:b #dt/e (coalesce :a 0.0)})]
      (is (= [1.0 0.0 3.0] (vec (result :b))))))
  (testing "coalesce — used in :where"
    (let [ds (ds/->dataset {:a [nil nil 3.0] :b [nil 2.0 4.0]})
          result (core/dt ds :where #dt/e (> (coalesce :a :b 0.0) 1.5))]
      (is (= 2 (ds/row-count result))))))

(deftest count-distinct-agg
  (testing "count-distinct — whole-table"
    (let [ds (ds/->dataset {:species ["Adelie" "Adelie" "Gentoo" "Chinstrap" "Gentoo"]})
          result (core/dt ds :agg {:n #dt/e (count-distinct :species)})]
      (is (= 3 (first (vec (result :n)))))))
  (testing "count-distinct — grouped"
    (let [ds (ds/->dataset {:species ["Adelie" "Adelie" "Gentoo" "Chinstrap" "Gentoo"]
                            :year [2007 2008 2007 2008 2007]})
          result (core/dt ds :by [:year] :agg {:n #dt/e (count-distinct :species)})]
      (is (= 2 (ds/row-count result)))
      (is (every? #(= 2 %) (vec (result :n)))))))

(deftest pass-nil-wrapper
  (testing "pass-nil — guards specified column, returns nil on missing"
    (let [ds (ds/->dataset {:x-str ["1" nil "3"]})
          result (core/dt ds :set {:x (core/pass-nil #(Integer/parseInt (:x-str %)) :x-str)})]
      (is (= [1 nil 3] (vec (result :x))))))
  (testing "pass-nil — multiple guard columns"
    (let [ds (ds/->dataset {:a [1.0 nil 3.0] :b [2.0 2.0 nil]})
          result (core/dt ds :set {:c (core/pass-nil #(+ (:a %) (:b %)) :a :b)})]
      (is (= [3.0 nil nil] (vec (result :c))))))
  (testing "pass-nil — no nil values, fn runs normally"
    (let [ds (ds/->dataset {:x ["1" "2" "3"]})
          result (core/dt ds :set {:n (core/pass-nil #(Integer/parseInt (:x %)) :x)})]
      (is (= [1 2 3] (vec (result :n))))))
  (testing "pass-nil — used in :where"
    (let [ds (ds/->dataset {:x-str ["1" nil "30"]})
          result (core/dt ds :where (core/pass-nil #(> (Integer/parseInt (:x-str %)) 5) :x-str))]
      (is (= 1 (ds/row-count result))))))

(deftest rename-fn
  (testing "rename renames columns without dropping any"
    (let [result (core/rename penguins {:mass :weight-kg :species :penguin-species})]
      (is (= #{:penguin-species :weight-kg :year} (set (ds/column-names result))))
      (is (= (ds/row-count penguins) (ds/row-count result))))))

;; ---------------------------------------------------------------------------
;; Window AST node parsing (#dt/e win/* integration)
;; ---------------------------------------------------------------------------

(deftest win-ast-parsing
  (testing "win/* symbols produce :win AST nodes"
    (let [ast #dt/e (win/rank :mass)]
      (is (= :win (:node/type ast)))
      (is (= :win/rank (:win/op ast)))
      (is (= :col (-> ast :win/args first :node/type)))
      (is (= :mass (-> ast :win/args first :col/name)))))

  (testing "win/lag parses column and offset args"
    (let [ast #dt/e (win/lag :mass 1)]
      (is (= :win/lag (:win/op ast)))
      (is (= 2 (count (:win/args ast))))
      (is (= :col (-> ast :win/args first :node/type)))
      (is (= :lit (-> ast :win/args second :node/type)))
      (is (= 1 (-> ast :win/args second :lit/value)))))

  (testing "win/* nested inside regular ops"
    (let [ast #dt/e (- :mass (win/lag :mass 1))]
      (is (= :op (:node/type ast)))
      (is (= :- (:op/name ast)))
      (is (= :win (-> ast :op/args second :node/type)))
      (is (= :win/lag (-> ast :op/args second :win/op)))))

  (testing "all 10 win/* symbols parse"
    (doseq [[sym expected] [['win/rank :win/rank]
                            ['win/dense-rank :win/dense-rank]
                            ['win/row-number :win/row-number]
                            ['win/lag :win/lag]
                            ['win/lead :win/lead]
                            ['win/cumsum :win/cumsum]
                            ['win/cummin :win/cummin]
                            ['win/cummax :win/cummax]
                            ['win/cummean :win/cummean]
                            ['win/rleid :win/rleid]]]
      (let [ast (datajure.expr/read-expr (list sym :mass))]
        (is (= :win (:node/type ast)) (str sym " should produce :win node"))
        (is (= expected (:win/op ast)) (str sym " -> " (:win/op ast)))))))

(deftest win-col-refs
  (testing "col-refs traverses :win nodes"
    (is (= #{:mass} (datajure.expr/col-refs #dt/e (win/rank :mass))))
    (is (= #{:mass} (datajure.expr/col-refs #dt/e (win/lag :mass 1))))
    (is (= #{:mass} (datajure.expr/col-refs #dt/e (- :mass (win/lag :mass 1))))))

  (testing "win-refs extracts window op keywords"
    (is (= #{:win/rank} (datajure.expr/win-refs #dt/e (win/rank :mass))))
    (is (= #{:win/lag} (datajure.expr/win-refs #dt/e (- :mass (win/lag :mass 1)))))
    (is (= #{} (datajure.expr/win-refs #dt/e (> :mass 4000))))))

(deftest win-context-validation
  (testing "win/* in :where throws :win-outside-window"
    (is (thrown-with-msg? Exception #"Window function"
                          (core/dt penguins :where #dt/e (> (win/rank :mass) 1))))
    (let [ed (try (core/dt penguins :where #dt/e (> (win/rank :mass) 1))
                  (catch Exception e (ex-data e)))]
      (is (= :win-outside-window (:dt/error ed)))
      (is (= #{:win/rank} (:dt/win-ops ed)))))

  (testing "win/* in :set without :by succeeds (whole-dataset window mode)"
    (core/reset-notes!)
    (let [result (core/dt penguins :set {:rank #dt/e (win/rank :mass)})]
      (is (ds/dataset? result))
      (is (contains? (set (ds/column-names result)) :rank))
      (is (= (ds/row-count penguins) (ds/row-count result)))))

  (testing "win/* in :agg throws :win-outside-window"
    (is (thrown-with-msg? Exception #"Window function"
                          (core/dt penguins :by [:species] :agg {:rank #dt/e (win/rank :mass)})))
    (is (thrown-with-msg? Exception #"Window function"
                          (core/dt penguins :agg {:rank #dt/e (win/rank :mass)}))))

  (testing "win/* nested in arithmetic in non-:set context"
    (is (thrown-with-msg? Exception #"Window function"
                          (core/dt penguins :where #dt/e (> (win/cumsum :mass) 10000)))))

  (testing "win/* in :by + :set (partitioned window mode) executes successfully"
    (core/reset-notes!)
    (let [result (core/dt penguins :by [:species] :set {:rank #dt/e (win/rank :mass)})]
      (is (ds/dataset? result))
      (is (contains? (set (ds/column-names result)) :rank)))))

(deftest within-order-invalid-errors
  (testing ":within-order without :set or :agg throws :within-order-invalid"
    (let [ed (try (core/dt penguins :within-order [(core/asc :mass)])
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (some? ed))
      (is (= :within-order-invalid (:dt/error ed)))))
  (testing ":within-order + :where only (no :set or :agg) still throws"
    (let [ed (try (core/dt penguins :where #dt/e (> :mass 0) :within-order [(core/asc :mass)])
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (some? ed))
      (is (= :within-order-invalid (:dt/error ed))))))

(deftest within-order-with-agg
  (testing ":within-order with :agg + :by enables OHLC in one call"
    (core/reset-notes!)
    (let [trades (ds/->dataset {:sym ["A" "A" "A" "B" "B" "B"]
                                :time [3 1 2 2 1 3]
                                :price [10.3 10.1 10.2 20.2 20.0 20.3]
                                :size [100 300 200 200 500 100]})
          result (core/dt trades
                          :by [:sym]
                          :within-order [(core/asc :time)]
                          :agg {:open #dt/e (first-val :price)
                                :close #dt/e (last-val :price)
                                :n core/N})
          rows (ds/mapseq-reader result)
          a-row (first (filter #(= "A" (:sym %)) rows))
          b-row (first (filter #(= "B" (:sym %)) rows))]
      (is (= 10.1 (:open a-row)))
      (is (= 10.3 (:close a-row)))
      (is (= 20.0 (:open b-row)))
      (is (= 20.3 (:close b-row)))))

  (testing ":within-order with :agg (no :by) sorts whole dataset before aggregation"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:date [3 1 2] :price [105.0 100.0 110.0]})
          result (core/dt ds
                          :within-order [(core/asc :date)]
                          :agg {:first-price #dt/e (first-val :price)
                                :last-price #dt/e (last-val :price)
                                :n core/N})]
      (is (= 100.0 (first (vec (:first-price result)))))
      (is (= 105.0 (first (vec (:last-price result)))))
      (is (= 3 (first (vec (:n result)))))))

  (testing ":within-order with :agg respects :desc"
    (core/reset-notes!)
    (let [trades (ds/->dataset {:sym ["A" "A" "A"] :time [1 2 3] :price [10.0 20.0 30.0]})
          result (core/dt trades
                          :by [:sym]
                          :within-order [(core/desc :time)]
                          :agg {:open #dt/e (first-val :price)
                                :close #dt/e (last-val :price)})]
      (is (= 30.0 (first (vec (:open result)))))
      (is (= 10.0 (first (vec (:close result)))))))

  (testing ":within-order is no-op for order-insensitive aggs"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "A"] :x [10.0 20.0 30.0]})
          r1 (core/dt ds :by [:sym] :agg {:s #dt/e (sm :x) :m #dt/e (mn :x)})
          r2 (core/dt ds :by [:sym]
                      :within-order [(core/desc :x)]
                      :agg {:s #dt/e (sm :x) :m #dt/e (mn :x)})]
      (is (= (vec (:s r1)) (vec (:s r2))))
      (is (= (vec (:m r1)) (vec (:m r2)))))))

(deftest agg-plain-fn-footgun
  (testing "plain fn returning a column via (:col %) throws structured error"
    (let [ds (ds/->dataset {:species ["A" "A" "B"] :mass [10 20 30]})
          ed (try (core/dt ds :by [:species] :agg {:x #(:mass %)})
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (some? ed))
      (is (= :agg-plain-fn-returned-non-scalar (:dt/error ed)))
      (is (= :x (:dt/column ed)))
      (is (= :column (:dt/returned-type ed)))))
  (testing "whole-table plain fn also catches the footgun (no :by)"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:mass [10 20 30]})
          ed (try (core/dt ds :agg {:x #(:mass %)})
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (some? ed))
      (is (= :agg-plain-fn-returned-non-scalar (:dt/error ed)))))
  (testing "plain fn returning a dataset throws with :returned-type :dataset"
    (let [ds (ds/->dataset {:species ["A" "A"] :mass [10 20]})
          ed (try (core/dt ds :by [:species] :agg {:x identity})
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (some? ed))
      (is (= :agg-plain-fn-returned-non-scalar (:dt/error ed)))
      (is (= :dataset (:dt/returned-type ed)))))
  (testing "correct plain fn (dfn/mean) works as before"
    (let [ds (ds/->dataset {:species ["A" "A" "B"] :mass [10 20 30]})
          result (core/dt ds :by [:species]
                          :agg {:avg (fn [sub] (tech.v3.datatype.functional/mean (:mass sub)))})
          rows (ds/mapseq-reader result)]
      (is (= 15.0 (:avg (first (filter #(= "A" (:species %)) rows)))))
      (is (= 30.0 (:avg (first (filter #(= "B" (:species %)) rows)))))))
  (testing "#dt/e agg unaffected by footgun check"
    (let [ds (ds/->dataset {:species ["A" "A" "B"] :mass [10 20 30]})
          result (core/dt ds :by [:species] :agg {:avg #dt/e (mn :mass)})
          rows (ds/mapseq-reader result)]
      (is (= 15.0 (:avg (first (filter #(= "A" (:species %)) rows)))))))
  (testing "plain fn returning a string scalar works (not a footgun)"
    (let [ds (ds/->dataset {:species ["A" "A" "B"] :mass [10 20 30]})
          result (core/dt ds :by [:species] :agg {:label (fn [_] "ok")})]
      (is (every? #(= "ok" %) (vec (:label result)))))))

(deftest nrow-alias
  (testing "nrow is equivalent to N in :agg"
    (let [ds (ds/->dataset {:species ["A" "A" "B"] :mass [10 20 30]})
          r1 (core/dt ds :by [:species] :agg {:n core/N})
          r2 (core/dt ds :by [:species] :agg {:n core/nrow})]
      (is (= (ds/mapseq-reader r1) (ds/mapseq-reader r2)))))
  (testing "nrow works in whole-table agg"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [1 2 3 4 5]})
          result (core/dt ds :agg {:count core/nrow})]
      (is (= 5 (first (vec (:count result))))))))

(deftest unknown-op-in-dt-e-error
  (testing "typo for base op gets structured error with suggestion"
    (let [ed (try (read-string "#dt/e (sqrt :x)")
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (some? ed))
      (is (= :unknown-op (:dt/error ed)))
      (is (= 'sqrt (:dt/op ed)))
      (is (contains? (set (:dt/suggestions ed)) 'sq))))
  (testing "typo for win/* op suggests the right namespaced op"
    (let [ed (try (read-string "#dt/e (win/mvag :x 20)")
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :unknown-op (:dt/error ed)))
      (is (contains? (set (:dt/suggestions ed)) 'win/mavg))))
  (testing "typo for row/* op suggests correct row function"
    (let [ed (try (read-string "#dt/e (row/sumz :a :b)")
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :unknown-op (:dt/error ed)))
      (is (contains? (set (:dt/suggestions ed)) 'row/sum))))
  (testing "typo for stat/* op suggests correct stat function"
    (let [ed (try (read-string "#dt/e (stat/standardise :x)")
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :unknown-op (:dt/error ed)))
      (is (contains? (set (:dt/suggestions ed)) 'stat/standardize))))
  (testing "nonsense op gets error without misleading suggestions"
    (let [ed (try (read-string "#dt/e (xxx :y)")
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= :unknown-op (:dt/error ed)))
      (is (nil? (:dt/suggestions ed)))))
  (testing "error message is human-readable"
    (let [msg (try (read-string "#dt/e (logg :x)")
                   nil
                   (catch clojure.lang.ExceptionInfo e (.getMessage e)))]
      (is (re-find #"Unknown op" msg))
      (is (re-find #"logg" msg))
      (is (re-find #"log" msg)))))

(deftest levenshtein-damerau-correctness
  (testing "single-char substitution: distance 1"
    (is (= 1 (#'core/levenshtein "cat" "bat")))
    (is (= 1 (#'datajure.expr/levenshtein "cat" "bat"))))
  (testing "single adjacent transposition: distance 1 (Damerau property)"
    (is (= 1 (#'core/levenshtein "hieght" "height")))
    (is (= 1 (#'datajure.expr/levenshtein "hieght" "height"))))
  (testing "classic Levenshtein: kitten -> sitting is distance 3"
    (is (= 3 (#'core/levenshtein "kitten" "sitting"))))
  (testing "empty strings"
    (is (= 0 (#'core/levenshtein "" "")))
    (is (= 3 (#'core/levenshtein "abc" "")))
    (is (= 3 (#'core/levenshtein "" "abc"))))
  (testing "identical strings: distance 0"
    (is (= 0 (#'core/levenshtein "height" "height"))))
  (testing "no transposition credit for non-adjacent swaps"
    ;; "acb" -> "bca" is 2 (two subs), not 1 — not adjacent
    (is (= 2 (#'core/levenshtein "acb" "bca"))))
  (testing "column typo suggestion reaches :height"
    (let [ds (ds/->dataset {:species ["A"] :height [40] :mass [3500]})
          ed (try (core/dt ds :set {:bmi #dt/e (/ :mass :hieght)})
                  nil
                  (catch clojure.lang.ExceptionInfo e (ex-data e)))]
      (is (= [:height] (get-in ed [:dt/closest :hieght]))))))

(deftest win-rank-basic
  (let [ds (ds/->dataset {:grp ["A" "A" "A" "A"] :val [10 20 20 30]})
        result (core/dt ds :by [:grp] :set {:rank #dt/e (win/rank :val)
                                            :dense #dt/e (win/dense-rank :val)
                                            :rownum #dt/e (win/row-number :val)})]
    (is (= [1 2 2 4] (vec (:rank result))))
    (is (= [1 2 2 3] (vec (:dense result))))
    (is (= [1 2 3 4] (vec (:rownum result))))))

(deftest win-lag-lead
  (let [ds (ds/->dataset {:grp ["A" "A" "A" "A"] :val [10 20 30 40]})
        result (core/dt ds :by [:grp] :set {:prev #dt/e (win/lag :val 1)
                                            :nxt #dt/e (win/lead :val 1)})]
    (is (= [nil 10 20 30] (vec (:prev result))))
    (is (= [20 30 40 nil] (vec (:nxt result))))))

(deftest win-cumulative-fns
  (let [ds (ds/->dataset {:grp ["A" "A" "A"] :val [10 20 30]})
        result (core/dt ds :by [:grp] :set {:csum #dt/e (win/cumsum :val)
                                            :cmin #dt/e (win/cummin :val)
                                            :cmax #dt/e (win/cummax :val)
                                            :cmean #dt/e (win/cummean :val)})]
    (is (= [10.0 30.0 60.0] (vec (:csum result))))
    (is (= [10.0 10.0 10.0] (vec (:cmin result))))
    (is (= [10.0 20.0 30.0] (vec (:cmax result))))
    (is (= [10.0 15.0 20.0] (vec (:cmean result)))))
  (testing "leading nils remain nil (B1 regression)"
    (let [ds (ds/->dataset {:grp ["A" "A" "A" "A"] :val [nil nil 5.0 3.0]})
          result (core/dt ds :by [:grp] :set {:cmin #dt/e (win/cummin :val)
                                              :cmax #dt/e (win/cummax :val)})]
      (is (= [nil nil 5.0 3.0] (vec (:cmin result))))
      (is (= [nil nil 5.0 5.0] (vec (:cmax result)))))))

(deftest empty-dataset-by
  (testing "empty dataset with :by + :agg returns empty dataset (not nil)"
    (let [ds (ds/->dataset {:species ["Adelie" "Gentoo"] :mass [4000 5000]})
          empty-ds (core/dt ds :where #dt/e (> :mass 9999))]
      (is (= 0 (ds/row-count empty-ds)))
      (is (ds/dataset? (core/dt empty-ds :by [:species] :agg {:n core/N})))
      (is (= 0 (ds/row-count (core/dt empty-ds :by [:species] :agg {:n core/N}))))
      (is (ds/dataset? (core/dt empty-ds :by [:species] :set {:mass2 #dt/e (* :mass 2)})))
      (is (= 0 (ds/row-count (core/dt empty-ds :by [:species] :set {:mass2 #dt/e (* :mass 2)})))))))

(deftest win-rleid-basic
  (let [ds (ds/->dataset {:grp [1 1 1 1 1 1 1]
                          :sign ["+" "+" "+" "-" "-" "+" "+"]})
        result (core/dt ds :by [:grp] :set {:regime #dt/e (win/rleid :sign)})]
    (is (= [1 1 1 2 2 3 3] (vec (:regime result))))))

(deftest win-composite-expression
  (testing "win/lag inside arithmetic — spec example: change = mass - lag(mass,1)"
    (let [ds (ds/->dataset {:grp ["A" "A" "A"] :val [100 150 130]})
          result (core/dt ds :by [:grp] :set {:chg #dt/e (- :val (win/lag :val 1))})]
      (is (= [nil 50 -20] (mapv #(when % (long %)) (:chg result)))))))

(deftest win-with-within-order
  (testing "rank respects :within-order sort direction"
    (let [ds (ds/->dataset {:grp ["A" "A" "A"] :val [20 10 30]})
          result (core/dt ds :by [:grp]
                          :within-order [(core/desc :val)]
                          :set {:rank #dt/e (win/rank :val)})]
      (is (= [1 2 3] (vec (:rank result))))
      (is (= [30 20 10] (vec (:val result)))))))

(deftest win-whole-dataset-window
  (testing "win/cumsum over whole dataset without :by"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:date [1 2 3 4 5] :price [100 102 101 105 103]})
          result (core/dt ds :set {:cum #dt/e (win/cumsum :price)})]
      (is (= [100.0 202.0 303.0 408.0 511.0] (vec (:cum result))))))

  (testing "win/lag without :by"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10 20 30]})
          result (core/dt ds :set {:prev #dt/e (win/lag :x 1)})]
      (is (nil? (first (vec (:prev result)))))
      (is (= 10 (nth (vec (:prev result)) 1)))
      (is (= 20 (nth (vec (:prev result)) 2)))))

  (testing "win/rank without :by with :within-order"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:val [30 10 20]})
          result (core/dt ds
                          :within-order [(core/asc :val)]
                          :set {:rank #dt/e (win/rank :val)})]
      (is (= [1 2 3] (vec (:rank result))))
      (is (= [10 20 30] (vec (:val result))))))

  (testing "composite win expression without :by"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:date [1 2 3] :price [100 110 105]})
          result (core/dt ds
                          :within-order [(core/asc :date)]
                          :set {:chg #dt/e (- :price (win/lag :price 1))})]
      (is (nil? (first (vec (:chg result)))))
      (is (= 10 (nth (vec (:chg result)) 1)))
      (is (= -5 (nth (vec (:chg result)) 2)))))

  (testing ":within-order without :by works (sorts then applies set)"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:date [3 1 2] :price [105 100 110]})
          result (core/dt ds
                          :within-order [(core/asc :date)]
                          :set {:cum #dt/e (win/cumsum :price)})]
      (is (= [100.0 210.0 315.0] (vec (:cum result))))
      (is (= [1 2 3] (vec (:date result))))))

  (testing ":within-order without :set or :agg throws"
    (is (thrown-with-msg? Exception #"requires :set or :agg"
                          (core/dt (ds/->dataset {:x [1]}) :within-order [(core/asc :x)]))))

  (testing ":within-order with :agg is now accepted (2.0.6)"
    (core/reset-notes!)
    (is (ds/dataset? (core/dt (ds/->dataset {:x [1]})
                              :within-order [(core/asc :x)]
                              :agg {:n core/N})))))

(deftest expr-composition-basic-reuse
  (testing "stored #dt/e expr works in :set"
    (let [bmi #dt/e (/ :mass 100)
          result (core/dt penguins :set {:bmi bmi})]
      (is (= 5 (ds/row-count result)))
      (is (= 37.5 (first (vec (:bmi result)))))))
  (testing "stored #dt/e expr works in :where"
    (let [big? #dt/e (> :mass 4000)
          result (core/dt penguins :where big?)]
      (is (= 2 (ds/row-count result)))
      (is (every? #(> % 4000) (vec (:mass result))))))
  (testing "stored #dt/e expr works in :agg"
    (let [avg-mass #dt/e (mn :mass)
          result (core/dt penguins :agg {:avg avg-mass})]
      (is (= 1 (ds/row-count result))))))

(deftest expr-composition-nested
  (testing "#dt/e (mn expr-var) — aggregation over composed expression"
    (let [bmi #dt/e (/ :mass 100)
          result (core/dt penguins :by [:species] :agg {:avg-bmi #dt/e (mn bmi)})]
      (is (= 3 (ds/row-count result)))
      (is (every? pos? (vec (:avg-bmi result))))))
  (testing "#dt/e (> expr-var literal) — comparison with composed expression"
    (let [bmi #dt/e (/ :mass 100)
          result (core/dt penguins :where #dt/e (> bmi 40))]
      (is (= 2 (ds/row-count result)))
      (is (every? #(> % 4000) (vec (:mass result))))))
  (testing "#dt/e (sd expr-var) — stddev over composed expression"
    (let [bmi #dt/e (/ :mass 100)
          result (core/dt penguins :agg {:sd-bmi #dt/e (sd bmi)})]
      (is (= 1 (ds/row-count result)))
      (is (pos? (first (vec (:sd-bmi result))))))))

(deftest expr-composition-col-refs
  (testing "col-refs traverses into composed expressions"
    (let [bmi #dt/e (/ :mass (sq :height))
          composed #dt/e (mn bmi)]
      (is (= #{:mass :height} (datajure.expr/col-refs composed)))))
  (testing "col-refs for non-composed lit returns empty"
    (is (= #{} (datajure.expr/col-refs {:node/type :lit :lit/value 42})))))

(deftest expr-composition-column-validation
  (testing "column validation catches typos in nested composed expressions"
    (let [bad #dt/e (/ :maas (sq :height))]
      (is (thrown-with-msg? Exception #"Unknown column"
                            (core/dt penguins :agg {:x #dt/e (mn bad)}))))))

(deftest row-ast-parsing
  (testing "row/* symbols produce :row AST nodes"
    (let [ast #dt/e (row/sum :q1 :q2)]
      (is (= :row (:node/type ast)))
      (is (= :row/sum (:row/op ast)))
      (is (= 2 (count (:row/args ast))))
      (is (= :col (-> ast :row/args first :node/type)))
      (is (= :q1 (-> ast :row/args first :col/name)))))

  (testing "all 6 row/* symbols parse"
    (doseq [[sym expected] [['row/sum :row/sum]
                            ['row/mean :row/mean]
                            ['row/min :row/min]
                            ['row/max :row/max]
                            ['row/count-nil :row/count-nil]
                            ['row/any-nil? :row/any-nil?]]]
      (let [ast (datajure.expr/read-expr (list sym :a :b))]
        (is (= :row (:node/type ast)) (str sym " should produce :row node"))
        (is (= expected (:row/op ast)) (str sym " -> " (:row/op ast))))))

  (testing "row/* nested inside regular ops"
    (let [ast #dt/e (> (row/sum :a :b) 10)]
      (is (= :op (:node/type ast)))
      (is (= :row (-> ast :op/args first :node/type))))))

(deftest row-col-refs
  (testing "col-refs traverses :row nodes"
    (is (= #{:q1 :q2 :q3} (datajure.expr/col-refs #dt/e (row/sum :q1 :q2 :q3))))
    (is (= #{:a :b} (datajure.expr/col-refs #dt/e (> (row/sum :a :b) 10)))))

  (testing "win-refs returns empty for row nodes"
    (is (= #{} (datajure.expr/win-refs #dt/e (row/sum :q1 :q2))))))

(deftest row-sum-basic
  (let [ds (ds/->dataset {:q1 [10 20 nil 40]
                          :q2 [1 nil 3 4]
                          :q3 [5 6 7 nil]})
        result (core/dt ds :set {:total #dt/e (row/sum :q1 :q2 :q3)})]
    (testing "nil treated as 0"
      (is (= 16.0 (nth (:total result) 0)))
      (is (= 26.0 (nth (:total result) 1)))
      (is (= 10.0 (nth (:total result) 2)))
      (is (= 44.0 (nth (:total result) 3)))))

  (testing "all-nil returns nil"
    (let [ds (ds/->dataset {:a [nil] :b [nil]})
          result (core/dt ds :set {:s #dt/e (row/sum :a :b)})]
      (is (nil? (nth (:s result) 0))))))

(deftest row-mean-basic
  (let [ds (ds/->dataset {:q1 [10.0 20.0 nil] :q2 [2.0 nil nil] :q3 [6.0 4.0 nil]})
        result (core/dt ds :set {:avg #dt/e (row/mean :q1 :q2 :q3)})]
    (testing "skips nil, averages non-nil"
      (is (= 6.0 (nth (:avg result) 0)))
      (is (= 12.0 (nth (:avg result) 1))))
    (testing "all-nil returns nil"
      (is (nil? (nth (:avg result) 2))))))

(deftest row-min-max-basic
  (let [ds (ds/->dataset {:a [10 nil 3] :b [5 7 nil] :c [8 nil nil]})
        result (core/dt ds :set {:lo #dt/e (row/min :a :b :c)
                                 :hi #dt/e (row/max :a :b :c)})]
    (testing "row/min skips nil"
      (is (= 5.0 (nth (:lo result) 0)))
      (is (= 7.0 (nth (:lo result) 1)))
      (is (= 3.0 (nth (:lo result) 2))))
    (testing "row/max skips nil"
      (is (= 10.0 (nth (:hi result) 0)))
      (is (= 7.0 (nth (:hi result) 1)))
      (is (= 3.0 (nth (:hi result) 2)))))

  (testing "all-nil returns nil"
    (let [ds (ds/->dataset {:a [nil] :b [nil]})
          result (core/dt ds :set {:lo #dt/e (row/min :a :b)
                                   :hi #dt/e (row/max :a :b)})]
      (is (nil? (nth (:lo result) 0)))
      (is (nil? (nth (:hi result) 0))))))

(deftest row-count-nil-and-any-nil
  (let [ds (ds/->dataset {:a [10 nil nil] :b [1 nil 3] :c [nil 6 7]})
        result (core/dt ds :set {:n-miss #dt/e (row/count-nil :a :b :c)
                                 :any #dt/e (row/any-nil? :a :b :c)})]
    (testing "row/count-nil counts missing"
      (is (= 1 (nth (:n-miss result) 0)))
      (is (= 2 (nth (:n-miss result) 1)))
      (is (= 1 (nth (:n-miss result) 2))))
    (testing "row/any-nil? detects missing"
      (is (= true (nth (:any result) 0)))
      (is (= true (nth (:any result) 1)))
      (is (= true (nth (:any result) 2)))))

  (testing "no nils present"
    (let [ds (ds/->dataset {:a [1 2] :b [3 4]})
          result (core/dt ds :set {:n #dt/e (row/count-nil :a :b)
                                   :any #dt/e (row/any-nil? :a :b)})]
      (is (= 0 (nth (:n result) 0)))
      (is (= false (nth (:any result) 0))))))

(deftest row-column-validation
  (testing "typo in row/* arg caught by column validation"
    (let [ds (ds/->dataset {:q1 [1] :q2 [2]})]
      (is (thrown-with-msg? Exception #"Unknown column"
                            (core/dt ds :set {:total #dt/e (row/sum :q1 :q999)}))))))

(deftest row-in-window-mode
  (testing "row/* works inside :by + :set window mode"
    (let [ds (ds/->dataset {:grp ["A" "A" "B" "B"]
                            :q1 [10 20 30 40]
                            :q2 [1 2 3 4]})
          result (core/dt ds :by [:grp] :set {:total #dt/e (row/sum :q1 :q2)})]
      (is (= [11.0 22.0 33.0 44.0] (vec (:total result)))))))

(deftest win-delta-ratio-differ
  (testing "win/delta: nil first, correct diffs"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0]})
          result (core/dt ds :set {:d #dt/e (win/delta :x)})]
      (is (nil? (first (vec (:d result)))))
      (is (= [10.0 10.0] (rest (vec (:d result)))))))

  (testing "win/ratio: nil first, correct ratios"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0]})
          result (core/dt ds :set {:r #dt/e (win/ratio :x)})]
      (is (nil? (first (vec (:r result)))))
      (is (= [2.0 1.5] (rest (vec (:r result)))))))

  (testing "win/differ: first element true, changes on value change"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:s ["A" "A" "B" "B" "A"]})
          result (core/dt ds :set {:chg #dt/e (win/differ :s)})]
      (is (= [true false true false true] (vec (:chg result))))))

  (testing "win/differ: all same -> only first is true"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:s ["A" "A" "A"]})
          result (core/dt ds :set {:chg #dt/e (win/differ :s)})]
      (is (= [true false false] (vec (:chg result))))))

  (testing "win/delta: nil in middle propagates"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 nil 30.0]})
          result (core/dt ds :set {:d #dt/e (win/delta :x)})]
      (is (every? nil? (vec (:d result))))))

  (testing "win/delta per partition — first of each partition is nil"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "B" "B"]
                            :price [100.0 110.0 50.0 55.0]})
          result (core/dt ds :by [:sym] :set {:d #dt/e (win/delta :price)})
          rows (ds/mapseq-reader result)
          a-rows (filter #(= "A" (:sym %)) rows)
          b-rows (filter #(= "B" (:sym %)) rows)]
      (is (nil? (:d (first a-rows))))
      (is (= 10.0 (:d (second a-rows))))
      (is (nil? (:d (first b-rows))))
      (is (= 5.0 (:d (second b-rows))))))

  (testing "win/ratio composite: simple return (- (win/ratio :price) 1)"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:price [100.0 110.0 105.0]})
          result (core/dt ds
                          :within-order [(core/asc :price)]
                          :set {:ret #dt/e (- (win/ratio :price) 1)})]
      (is (nil? (first (vec (:ret result)))))
      (is (some? (second (vec (:ret result)))))))

  (testing "win/differ with :within-order"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:date [3 1 2] :signal ["B" "A" "A"]})
          result (core/dt ds
                          :within-order [(core/asc :date)]
                          :set {:chg #dt/e (win/differ :signal)})]
      (is (= [true false true] (vec (:chg result)))))))

(deftest win-ratio-zero-guard
  (testing "zero denominator produces nil, not Infinity"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:price [100.0 0.0 50.0 100.0]})
          result (core/dt ds :set {:r #dt/e (win/ratio :price)})
          vals (vec (:r result))]
      (is (nil? (first vals)))
      (is (= 0.0 (second vals)))
      (is (nil? (nth vals 2)))
      (is (= 2.0 (nth vals 3)))))

  (testing "simple-return idiom handles zero prices cleanly"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:price [100.0 0.0 50.0 100.0]})
          result (core/dt ds :set {:ret #dt/e (- (win/ratio :price) 1)})
          vals (vec (:ret result))]
      (is (nil? (first vals)))
      (is (= -1.0 (second vals)))
      (is (nil? (nth vals 2)))
      (is (= 1.0 (nth vals 3)))))

  (testing "no Infinity leaks into result"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:price [10.0 0.0 5.0]})
          result (core/dt ds :set {:r #dt/e (win/ratio :price)})]
      (is (every? (fn [v]
                    (or (nil? v)
                        (and (number? v) (Double/isFinite (double v)))))
                  (vec (:r result))))))

  (testing "per-partition: zero in one group doesn't affect others"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "B" "B"]
                            :price [100.0 0.0 50.0 100.0]})
          result (core/dt ds :by [:sym] :set {:r #dt/e (win/ratio :price)})
          rows (ds/mapseq-reader result)
          a-ratios (map :r (filter #(= "A" (:sym %)) rows))
          b-ratios (map :r (filter #(= "B" (:sym %)) rows))]
      (is (nil? (first a-ratios)))
      (is (= 0.0 (second a-ratios)))
      (is (nil? (first b-ratios)))
      (is (= 2.0 (second b-ratios))))))

(deftest win-rolling-fns
  (testing "win/mavg: expanding window at start, correct values"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0 40.0 50.0]})
          result (core/dt ds :set {:ma #dt/e (win/mavg :x 3)})]
      (is (= [10.0 15.0 20.0 30.0 40.0] (vec (:ma result))))))

  (testing "win/msum: expanding window at start, correct values"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0 40.0 50.0]})
          result (core/dt ds :set {:ms #dt/e (win/msum :x 3)})]
      (is (= [10.0 30.0 60.0 90.0 120.0] (vec (:ms result))))))

  (testing "win/mdev: population std dev, expanding window"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0 40.0 50.0]})
          result (core/dt ds :set {:md #dt/e (win/mdev :x 3)})
          vals (vec (:md result))]
      (is (= 0.0 (first vals)))
      (is (= 5.0 (second vals)))
      (is (< (Math/abs (- (nth vals 2) 8.165)) 0.001))))

  (testing "win/mmin: moving minimum"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [30.0 10.0 50.0 20.0 40.0]})
          result (core/dt ds :set {:mm #dt/e (win/mmin :x 3)})]
      (is (= [30.0 10.0 10.0 10.0 20.0] (vec (:mm result))))))

  (testing "win/mmax: moving maximum"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [30.0 10.0 50.0 20.0 40.0]})
          result (core/dt ds :set {:mm #dt/e (win/mmax :x 3)})]
      (is (= [30.0 30.0 50.0 50.0 50.0] (vec (:mm result))))))

  (testing "nil values skipped within window"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 nil 30.0]})
          result (core/dt ds :set {:ma #dt/e (win/mavg :x 3)})]
      (is (= [10.0 10.0 20.0] (vec (:ma result))))))

  (testing "all-nil window returns nil"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [nil nil nil]})
          result (core/dt ds :set {:ma #dt/e (win/mavg :x 3)})]
      (is (every? nil? (vec (:ma result))))))

  (testing "width=1 equals column itself"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0]})
          result (core/dt ds :set {:ma #dt/e (win/mavg :x 1)})]
      (is (= [10.0 20.0 30.0] (vec (:ma result))))))

  (testing "per-partition: each group has independent expanding window"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "A" "B" "B"]
                            :price [10.0 20.0 30.0 100.0 80.0]})
          result (core/dt ds :by [:sym] :set {:ma #dt/e (win/mavg :price 2)})
          rows (ds/mapseq-reader result)
          a-mas (map :ma (filter #(= "A" (:sym %)) rows))
          b-mas (map :ma (filter #(= "B" (:sym %)) rows))]
      (is (= [10.0 15.0 25.0] a-mas))
      (is (= [100.0 90.0] b-mas))))

  (testing "win/mavg with :within-order"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:date [3 1 2] :x [30.0 10.0 20.0]})
          result (core/dt ds
                          :within-order [(core/asc :date)]
                          :set {:ma #dt/e (win/mavg :x 2)})]
      (is (= [10.0 15.0 25.0] (vec (:ma result))))))

  (testing "composite: mavg used in arithmetic expression"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0]})
          result (core/dt ds :set {:diff #dt/e (- :x (win/mavg :x 2))})]
      (is (= [0.0 5.0 5.0] (vec (:diff result)))))))

(deftest win-ema-fills
  (testing "win/ema: period dispatch, correct values"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0]})
          result (core/dt ds :set {:e #dt/e (win/ema :x 2)})
          vals (vec (:e result))]
      (is (= 10.0 (first vals)))
      (is (< (Math/abs (- (second vals) 16.667)) 0.001))
      (is (< (Math/abs (- (nth vals 2) 25.556)) 0.001))))

  (testing "win/ema: alpha dispatch produces same result as equivalent period"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 20.0 30.0]})
          r-period (core/dt ds :set {:e #dt/e (win/ema :x 2)})
          r-alpha (core/dt ds :set {:e #dt/e (win/ema :x 0.6667)})]
      (is (< (Math/abs (- (first (vec (:e r-period))) (first (vec (:e r-alpha))))) 0.001))
      (is (< (Math/abs (- (second (vec (:e r-period))) (second (vec (:e r-alpha))))) 0.001))))

  (testing "win/ema: nil in middle carries forward last ema"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [10.0 nil 30.0]})
          result (core/dt ds :set {:e #dt/e (win/ema :x 2)})
          vals (vec (:e result))]
      (is (= 10.0 (first vals)))
      (is (= 10.0 (second vals)))
      (is (some? (nth vals 2)))))

  (testing "win/ema: leading nils remain nil"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [nil nil 10.0 20.0]})
          result (core/dt ds :set {:e #dt/e (win/ema :x 2)})
          vals (vec (:e result))]
      (is (nil? (first vals)))
      (is (nil? (second vals)))
      (is (= 10.0 (nth vals 2)))
      (is (some? (nth vals 3)))))

  (testing "win/ema: per-partition independence"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "B" "B"]
                            :x [10.0 20.0 100.0 200.0]})
          result (core/dt ds :by [:sym] :set {:e #dt/e (win/ema :x 2)})
          rows (ds/mapseq-reader result)
          a-first (:e (first (filter #(= "A" (:sym %)) rows)))
          b-first (:e (first (filter #(= "B" (:sym %)) rows)))]
      (is (= 10.0 a-first))
      (is (= 100.0 b-first))))

  (testing "win/fills: basic forward-fill"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [1.0 nil nil 4.0 nil]})
          result (core/dt ds :set {:f #dt/e (win/fills :x)})]
      (is (= [1.0 1.0 1.0 4.0 4.0] (vec (:f result))))))

  (testing "win/fills: leading nils remain nil"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [nil nil 3.0 nil 5.0]})
          result (core/dt ds :set {:f #dt/e (win/fills :x)})
          vals (vec (:f result))]
      (is (nil? (first vals)))
      (is (nil? (second vals)))
      (is (= [3.0 3.0 5.0] (drop 2 vals)))))

  (testing "win/fills: no nils — passthrough"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [1.0 2.0 3.0]})
          result (core/dt ds :set {:f #dt/e (win/fills :x)})]
      (is (= [1.0 2.0 3.0] (vec (:f result))))))

  (testing "win/fills: per-partition — each group fills independently"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "A" "B" "B"]
                            :x [1.0 nil 3.0 nil 5.0]})
          result (core/dt ds :by [:sym] :set {:f #dt/e (win/fills :x)})
          rows (ds/mapseq-reader result)
          a-fs (map :f (filter #(= "A" (:sym %)) rows))
          b-fs (map :f (filter #(= "B" (:sym %)) rows))]
      (is (= [1.0 1.0 3.0] a-fs))
      (is (= [nil 5.0] b-fs))))

  (testing "win/fills: with :within-order — sort determines fill direction"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:date [3 1 2] :x [10.0 nil nil]})
          result (core/dt ds
                          :within-order [(core/asc :date)]
                          :set {:f #dt/e (win/fills :x)})
          vals (vec (:f result))]
      (is (nil? (first vals)))
      (is (nil? (second vals)))
      (is (= 10.0 (nth vals 2))))))

;; ---------------------------------------------------------------------------
;; Aggregation helpers: first-val, last-val, wavg, wsum
;; ---------------------------------------------------------------------------

(deftest first-val-basic
  (testing "first-val returns the first value in a group"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "A" "B" "B"]
                            :price [10.0 12.0 11.0 5.0 6.0]})
          result (core/dt ds :by [:sym] :agg {:open #dt/e (first-val :price)})
          rows (into {} (map (fn [r] [(:sym r) (:open r)]) (ds/mapseq-reader result)))]
      (is (= 10.0 (rows "A")))
      (is (= 5.0 (rows "B"))))))

(deftest last-val-basic
  (testing "last-val returns the last value in a group"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "A" "B" "B"]
                            :price [10.0 12.0 11.0 5.0 6.0]})
          result (core/dt ds :by [:sym] :agg {:close #dt/e (last-val :price)})
          rows (into {} (map (fn [r] [(:sym r) (:close r)]) (ds/mapseq-reader result)))]
      (is (= 11.0 (rows "A")))
      (is (= 6.0 (rows "B"))))))

(deftest first-last-val-whole-table
  (testing "first-val / last-val work on whole table without :by"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [1.0 2.0 3.0]})
          r (core/dt ds :agg {:first #dt/e (first-val :x)
                              :last #dt/e (last-val :x)})]
      (is (= 1.0 (first (vec (:first r)))))
      (is (= 3.0 (first (vec (:last r))))))))

(deftest wavg-basic
  (testing "wavg computes weighted average (VWAP)"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "A" "B" "B"]
                            :price [10.0 12.0 11.0 5.0 6.0]
                            :size [100.0 200.0 150.0 300.0 100.0]})
          result (core/dt ds :by [:sym] :agg {:vwap #dt/e (wavg :size :price)})
          rows (into {} (map (fn [r] [(:sym r) (:vwap r)]) (ds/mapseq-reader result)))]
      (is (< (Math/abs (- (rows "A") (/ 5050.0 450.0))) 1e-9))
      (is (< (Math/abs (- (rows "B") (/ 2100.0 400.0))) 1e-9)))))

(deftest wsum-basic
  (testing "wsum computes weighted sum"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:sym ["A" "A" "B" "B"]
                            :w [1.0 2.0 3.0 4.0]
                            :v [10.0 20.0 30.0 40.0]})
          result (core/dt ds :by [:sym] :agg {:ws #dt/e (wsum :w :v)})
          rows (into {} (map (fn [r] [(:sym r) (:ws r)]) (ds/mapseq-reader result)))]
      (is (= 50.0 (rows "A"))) ;; 1*10 + 2*20
      (is (= 250.0 (rows "B")))))) ;; 3*30 + 4*40

(deftest wavg-nil-handling
  (testing "wavg skips rows where weight or value is nil"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:w [1.0 nil 2.0] :v [10.0 20.0 nil]})
          result (core/dt ds :agg {:wa #dt/e (wavg :w :v)})]
      (is (= 10.0 (first (vec (:wa result))))))))

(deftest div0-basic
  (testing "div0 returns float division when denominator is non-zero"
    (let [ds (ds/->dataset {:a [10.0 6.0] :b [2.0 3.0]})
          result (core/dt ds :set {:r #dt/e (div0 :a :b)})]
      (is (= [5.0 2.0] (vec (:r result)))))))

(deftest div0-zero-denominator
  (testing "div0 returns nil when denominator is zero"
    (let [ds (ds/->dataset {:a [6.0] :b [0.0]})
          result (core/dt ds :set {:r #dt/e (div0 :a :b)})]
      (is (nil? (first (vec (:r result))))))))

(deftest div0-nil-denominator
  (testing "div0 returns nil when denominator is nil"
    (let [ds (ds/->dataset {:a [6.0] :b [nil]})
          result (core/dt ds :set {:r #dt/e (div0 :a :b)})]
      (is (nil? (first (vec (:r result))))))))

(deftest div0-nil-numerator
  (testing "div0 returns nil when numerator is nil"
    (let [ds (ds/->dataset {:a [nil] :b [2.0]})
          result (core/dt ds :set {:r #dt/e (div0 :a :b)})]
      (is (nil? (first (vec (:r result))))))))

(deftest div0-mixed
  (testing "div0 handles a mixed column correctly"
    (let [ds (ds/->dataset {:a [10.0 nil 6.0 0.0] :b [2.0 2.0 0.0 nil]})
          result (core/dt ds :set {:r #dt/e (div0 :a :b)})
          r (vec (:r result))]
      (is (= 5.0 (first r)))
      (is (nil? (nth r 1)))
      (is (nil? (nth r 2)))
      (is (nil? (nth r 3))))))

(deftest div0-scalar-denominator
  (testing "div0 works with scalar (literal) denominator"
    (let [ds (ds/->dataset {:a [0.0 10.0 nil 30.0]})
          result (core/dt ds :set {:r #dt/e (div0 :a 2)})
          r (vec (:r result))]
      (is (= 0.0 (nth r 0)))
      (is (= 5.0 (nth r 1)))
      (is (nil? (nth r 2)))
      (is (= 15.0 (nth r 3)))))
  (testing "div0 returns nil for all rows when scalar denominator is zero"
    (let [ds (ds/->dataset {:a [10.0 20.0]})
          result (core/dt ds :set {:r #dt/e (div0 :a 0)})
          r (vec (:r result))]
      (is (nil? (nth r 0)))
      (is (nil? (nth r 1))))))

(deftest win-scan-basic
  (testing "win/scan +: cumulative sum equivalent"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [1.0 2.0 3.0 4.0]})
          result (core/dt ds :set {:cs #dt/e (win/scan + :x)})]
      (is (= [1.0 3.0 6.0 10.0] (vec (:cs result))))))

  (testing "win/scan *: cumulative product (wealth index)"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:r [1.1 1.2 1.3]})
          result (core/dt ds :set {:w #dt/e (win/scan * :r)})
          vals (vec (:w result))]
      (is (= 1.1 (first vals)))
      (is (< (Math/abs (- (* 1.1 1.2) (second vals))) 1e-9))
      (is (< (Math/abs (- (* 1.1 1.2 1.3) (nth vals 2))) 1e-9))))

  (testing "win/scan max: running maximum"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [30.0 10.0 50.0 20.0]})
          result (core/dt ds :set {:rm #dt/e (win/scan max :x)})]
      (is (= [30.0 30.0 50.0 50.0] (vec (:rm result))))))

  (testing "win/scan min: running minimum"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [30.0 10.0 50.0 20.0]})
          result (core/dt ds :set {:rm #dt/e (win/scan min :x)})]
      (is (= [30.0 10.0 10.0 10.0] (vec (:rm result))))))

  (testing "win/scan nil values: leading nils remain nil, mid-series skipped"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [nil 1.0 nil 3.0]})
          result (core/dt ds :set {:cs #dt/e (win/scan + :x)})
          vals (vec (:cs result))]
      (is (nil? (first vals)))
      (is (= 1.0 (second vals)))
      (is (= 1.0 (nth vals 2)))
      (is (= 4.0 (nth vals 3)))))

  (testing "win/scan per-partition with :by"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:grp ["A" "A" "B" "B"] :x [1.0 2.0 3.0 4.0]})
          result (core/dt ds :by [:grp] :set {:cs #dt/e (win/scan + :x)})
          rows (ds/mapseq-reader result)
          a-rows (filter #(= "A" (:grp %)) rows)
          b-rows (filter #(= "B" (:grp %)) rows)]
      (is (= 1.0 (:cs (first a-rows))))
      (is (= 3.0 (:cs (second a-rows))))
      (is (= 3.0 (:cs (first b-rows))))
      (is (= 7.0 (:cs (second b-rows))))))

  (testing "win/scan wealth index: (win/scan * (+ 1 :ret))"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:ret [0.1 0.2 -0.1]})
          result (core/dt ds :set {:w #dt/e (win/scan * (+ 1 :ret))})
          vals (vec (:w result))]
      (is (< (Math/abs (- 1.1 (first vals))) 1e-9))
      (is (< (Math/abs (- (* 1.1 1.2) (second vals))) 1e-9))
      (is (< (Math/abs (- (* 1.1 1.2 0.9) (nth vals 2))) 1e-9))))

  (testing "win/scan col-refs traversal for column validation"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [1.0 2.0]})]
      (is (thrown-with-msg? Exception #"Unknown column"
                            (core/dt ds :set {:cs #dt/e (win/scan + :zzz)})))))

  (testing "win/scan win-refs: detected as window function"
    (let [node #dt/e (win/scan + :x)]
      (is (contains? (datajure.expr/win-refs node) :win/scan))))

  (testing "win/scan runtime error on unknown op"
    (is (thrown? Exception
                 (datajure.window/win-scan :foo (tech.v3.datatype/->reader [1 2 3])))))

  (testing "win/scan :within-order interaction"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:x [3.0 1.0 2.0]})
          result (core/dt ds :within-order [(core/asc :x)] :set {:cs #dt/e (win/scan + :x)})]
      (is (= [1.0 3.0 6.0] (vec (:cs result)))))))

(deftest xbar-ast-parsing
  (testing "numeric xbar: :xbar node with nil unit"
    (let [node #dt/e (xbar :price 10)]
      (is (= :xbar (:node/type node)))
      (is (= :price (-> node :xbar/col :col/name)))
      (is (= 10 (-> node :xbar/width :lit/value)))
      (is (nil? (:xbar/unit node)))))
  (testing "temporal xbar: :xbar node with unit keyword"
    (let [node #dt/e (xbar :t 5 :minutes)]
      (is (= :xbar (:node/type node)))
      (is (= :t (-> node :xbar/col :col/name)))
      (is (= 5 (-> node :xbar/width :lit/value)))
      (is (= :minutes (:xbar/unit node))))))

(deftest xbar-col-refs
  (testing "col-refs extracts column from xbar"
    (is (= #{:price} (datajure.expr/col-refs #dt/e (xbar :price 10)))))
  (testing "win-refs returns empty for xbar (not a window function)"
    (is (= #{} (datajure.expr/win-refs #dt/e (xbar :price 10)))))
  (testing "col-refs traverses xbar in composite expression"
    (is (= #{:price :vol} (datajure.expr/col-refs #dt/e (+ (xbar :price 10) :vol))))))

(deftest xbar-numeric-basic
  (testing "xbar in :set produces floored buckets"
    (core/reset-notes!)
    (let [data (ds/->dataset {:price [3 7 12 18 22 28 35 41]})
          result (core/dt data :set {:bucket #dt/e (xbar :price 10)})]
      (is (= [0 0 10 10 20 20 30 40] (vec (:bucket result))))))
  (testing "xbar width 5"
    (core/reset-notes!)
    (let [data (ds/->dataset {:x [1 4 5 9 10 14]})
          result (core/dt data :set {:b #dt/e (xbar :x 5)})]
      (is (= [0 0 5 5 10 10] (vec (:b result)))))))

(deftest xbar-numeric-in-by-agg
  (testing "xbar in :by groups rows into correct buckets, group key named after column"
    (core/reset-notes!)
    (let [data (ds/->dataset {:price [3 7 12 18 22 28 35 41]
                              :vol [1 2 3 4 5 6 7 8]})
          result (core/dt data :by [(core/xbar :price 10)] :agg {:n core/N :total #dt/e (sm :vol)})
          rows (sort-by :price (ds/mapseq-reader result))]
      (is (= 5 (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :price))
      (is (not (contains? (set (ds/column-names result)) :xbar-0)))
      (is (= {:price 0 :n 2 :total 3.0} (select-keys (nth rows 0) [:price :n :total])))
      (is (= {:price 10 :n 2 :total 7.0} (select-keys (nth rows 1) [:price :n :total])))
      (is (= {:price 20 :n 2 :total 11.0} (select-keys (nth rows 2) [:price :n :total])))
      (is (= {:price 30 :n 1 :total 7.0} (select-keys (nth rows 3) [:price :n :total])))
      (is (= {:price 40 :n 1 :total 8.0} (select-keys (nth rows 4) [:price :n :total]))))))

(deftest xbar-by-mixed-keyword-and-fn
  (testing ":by with xbar fn + keyword produces correct groups, xbar col named after column"
    (core/reset-notes!)
    (let [data (ds/->dataset {:sym ["A" "A" "A" "A" "B" "B" "B" "B"]
                              :price [3 7 12 18 2 8 15 21]
                              :vol [1 2 3 4 5 6 7 8]})
          result (core/dt data :by [(core/xbar :price 10) :sym] :agg {:n core/N})
          rows (ds/mapseq-reader result)]
      (is (= 5 (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :price))
      (is (not (contains? (set (ds/column-names result)) :xbar-0)))
      (is (= 2 (:n (first (filter #(and (= "A" (:sym %)) (= 0 (:price %))) rows)))))
      (is (= 2 (:n (first (filter #(and (= "A" (:sym %)) (= 10 (:price %))) rows)))))
      (is (= 2 (:n (first (filter #(and (= "B" (:sym %)) (= 0 (:price %))) rows)))))
      (is (= 1 (:n (first (filter #(and (= "B" (:sym %)) (= 10 (:price %))) rows)))))
      (is (= 1 (:n (first (filter #(and (= "B" (:sym %)) (= 20 (:price %))) rows))))))))

(deftest xbar-by-plain-fn-group-key
  (testing "plain fn in :by gets :fn-N fallback key, not :xbar-N"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:price [3 7 12 18] :vol [1 2 3 4]})
          result (core/dt ds
                          :by [(fn [row] (> (:price row) 10))]
                          :agg {:n core/N})]
      (is (contains? (set (ds/column-names result)) :fn-0))
      (is (not (contains? (set (ds/column-names result)) :xbar-0)))))
  (testing "plain fn with :datajure/col metadata uses that name as group key"
    (core/reset-notes!)
    (let [ds (ds/->dataset {:price [3 7 12 18] :vol [1 2 3 4]})
          big-fn (with-meta (fn [row] (> (:price row) 10)) {:datajure/col :big?})
          result (core/dt ds :by [big-fn] :agg {:n core/N})]
      (is (contains? (set (ds/column-names result)) :big?))
      (is (not (contains? (set (ds/column-names result)) :fn-0))))))

(deftest xbar-nil-handling
  (testing "nil value in #dt/e xbar produces nil bucket, not crash"
    (core/reset-notes!)
    (let [data (ds/->dataset {:price [10 nil 30]})
          result (core/dt data :set {:b #dt/e (xbar :price 10)})
          vals (vec (:b result))]
      (is (= 10 (nth vals 0)))
      (is (nil? (nth vals 1)))
      (is (= 30 (nth vals 2)))))
  (testing "nil value in standalone xbar fn produces nil, not crash"
    (let [f (core/xbar :price 10)]
      (is (nil? (f {:price nil})))
      (is (= 10 (f {:price 15}))))))

(deftest xbar-temporal-minutes
  (testing "xbar :minutes buckets LocalDateTime values correctly"
    (core/reset-notes!)
    (let [times [(java.time.LocalDateTime/of 2024 1 1 9 30 0)
                 (java.time.LocalDateTime/of 2024 1 1 9 33 0)
                 (java.time.LocalDateTime/of 2024 1 1 9 37 0)
                 (java.time.LocalDateTime/of 2024 1 1 9 41 0)
                 (java.time.LocalDateTime/of 2024 1 1 9 46 0)]
          data (ds/->dataset {:t times :vol [10 20 30 40 50]})
          result (core/dt data :set {:b #dt/e (xbar :t 5 :minutes)})
          buckets (vec (:b result))]
      ;; 9:30 and 9:33 fall in the same 5-minute bucket
      (is (= (nth buckets 0) (nth buckets 1)))
      ;; 9:37, 9:41, 9:46 are each in different buckets
      (is (not= (nth buckets 1) (nth buckets 2)))
      (is (not= (nth buckets 2) (nth buckets 3)))
      (is (not= (nth buckets 3) (nth buckets 4)))
      ;; all buckets are multiples of 5
      (is (every? #(zero? (mod % 5)) buckets)))))

(deftest xbar-temporal-in-by
  (testing "xbar :minutes in :by groups trades into correct bars"
    (core/reset-notes!)
    (let [times [(java.time.LocalDateTime/of 2024 1 1 9 30 0)
                 (java.time.LocalDateTime/of 2024 1 1 9 33 0)
                 (java.time.LocalDateTime/of 2024 1 1 9 37 0)
                 (java.time.LocalDateTime/of 2024 1 1 9 41 0)
                 (java.time.LocalDateTime/of 2024 1 1 9 46 0)]
          data (ds/->dataset {:t times :vol [10 20 30 40 50]})
          result (core/dt data :by [(core/xbar :t 5 :minutes)] :agg {:n core/N :total #dt/e (sm :vol)})]
      ;; 9:30+9:33 → 1 bar, 9:37, 9:41, 9:46 → 3 separate bars = 4 total
      (is (= 4 (ds/row-count result)))
      ;; bar with 2 rows has vol sum 30
      (let [bar-of-2 (first (filter #(= 2 (:n %)) (ds/mapseq-reader result)))]
        (is (some? bar-of-2))
        (is (= 30.0 (:total bar-of-2)))))))

(deftest xbar-unknown-unit-error
  (testing "unknown temporal unit throws :xbar-unknown-unit"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unknown xbar temporal unit"
                          (core/xbar :t 5 :fortnights)))))

(deftest xbar-standalone-fn
  (testing "standalone numeric xbar returns correct bucket"
    (let [f (core/xbar :price 10)]
      (is (= 0 (f {:price 3})))
      (is (= 0 (f {:price 9})))
      (is (= 10 (f {:price 10})))
      (is (= 10 (f {:price 17})))
      (is (= 30 (f {:price 35})))))
  (testing "standalone xbar returns nil for nil value"
    (let [f (core/xbar :price 10)]
      (is (nil? (f {:price nil})))))
  (testing "standalone xbar composes in :by with keyword"
    (core/reset-notes!)
    (let [data (ds/->dataset {:price [5 8 25] :sym ["A" "A" "B"]})
          result (core/dt data :by [(core/xbar :price 10) :sym] :agg {:n core/N})]
      (is (= 2 (ds/row-count result))))))

(deftest cut-ast-parsing
  (testing "cut node has correct type, col, and n"
    (let [node #dt/e (cut :mass 4)]
      (is (= :cut (:node/type node)))
      (is (= :mass (-> node :cut/col :col/name)))
      (is (= 4 (-> node :cut/n :lit/value)))))
  (testing "cut with n=1"
    (let [node #dt/e (cut :x 1)]
      (is (= 1 (-> node :cut/n :lit/value))))))

(deftest cut-col-refs
  (testing "col-refs extracts column from cut"
    (is (= #{:mass} (datajure.expr/col-refs #dt/e (cut :mass 4)))))
  (testing "win-refs returns empty for cut"
    (is (= #{} (datajure.expr/win-refs #dt/e (cut :mass 4)))))
  (testing "col-refs traverses cut in composite expression"
    (is (= #{:mass :ret} (datajure.expr/col-refs #dt/e (+ (cut :mass 4) :ret))))))

(deftest cut-basic
  (testing "cut produces bins 1..n for uniform data"
    (core/reset-notes!)
    (let [data (ds/->dataset {:x [1 2 3 4 5 6 7 8 9 10]})
          result (core/dt data :set {:q #dt/e (cut :x 4)})
          qs (vec (:q result))]
      (is (every? #(<= 1 % 4) qs))
      (is (some #(= 1 %) qs))
      (is (some #(= 4 %) qs))))
  (testing "cut with n=1 assigns everything to bin 1"
    (core/reset-notes!)
    (let [data (ds/->dataset {:x [10 20 30 40]})
          result (core/dt data :set {:q #dt/e (cut :x 1)})]
      (is (= [1 1 1 1] (vec (:q result))))))
  (testing "cut with n=2 splits at median"
    (core/reset-notes!)
    (let [data (ds/->dataset {:x [10 20 30 40 50 60 70 80]})
          result (core/dt data :set {:q #dt/e (cut :x 2)})
          qs (vec (:q result))]
      (is (= 4 (count (filter #(= 1 %) qs))))
      (is (= 4 (count (filter #(= 2 %) qs)))))))

(deftest cut-nil-handling
  (testing "nil values produce nil bins"
    (core/reset-notes!)
    (let [data (ds/->dataset {:x [10 nil 30 nil 50]})
          result (core/dt data :set {:q #dt/e (cut :x 2)})
          qs (vec (:q result))]
      (is (nil? (nth qs 1)))
      (is (nil? (nth qs 3)))
      (is (some? (nth qs 0)))
      (is (some? (nth qs 2)))
      (is (some? (nth qs 4))))))

(deftest cut-column-validation
  (testing "unknown column in cut throws :unknown-column"
    (let [data (ds/->dataset {:x [1 2 3]})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (core/dt data :set {:q #dt/e (cut :zzz 4)}))))))

(deftest cut-standalone-throws
  (testing "calling cut standalone throws :cut-standalone-not-supported"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo
                          #"cut requires whole-column context"
                          (core/cut :mass 4)))))

(deftest cut-in-where
  (testing "cut in :where filters to bottom quartile"
    (core/reset-notes!)
    (let [data (ds/->dataset {:x [10 20 30 40 50 60 70 80]})
          result (core/dt data :where #dt/e (= (cut :x 4) 1))]
      (is (= 2 (ds/row-count result)))
      (is (every? #(<= % 20) (vec (:x result)))))))

(deftest cut-from-ast-parsing
  (testing ":cut/from node is a predicate expression, not a column ref"
    (let [node #dt/e (cut :mass 5 :from (= :exchcd 1))]
      (is (= :cut (:node/type node)))
      (is (= :mass (-> node :cut/col :col/name)))
      (is (= 5 (-> node :cut/n :lit/value)))
      (is (= :op (-> node :cut/from :node/type)))
      (is (= := (-> node :cut/from :op/name)))))
  (testing ":from with a simple column keyword produces a :col predicate"
    (let [node #dt/e (cut :mass 5 :from :nyse?)]
      (is (= :col (-> node :cut/from :node/type)))
      (is (= :nyse? (-> node :cut/from :col/name)))))
  (testing "no :from leaves :cut/from nil"
    (let [node #dt/e (cut :mass 5)]
      (is (nil? (:cut/from node))))))

(deftest cut-from-col-refs
  (testing "col-refs includes cols from :from predicate expression"
    (is (= #{:mass :exchcd}
           (datajure.expr/col-refs #dt/e (cut :mass 5 :from (= :exchcd 1))))))
  (testing "col-refs with :from as column keyword includes both cols"
    (is (= #{:mass :nyse?}
           (datajure.expr/col-refs #dt/e (cut :mass 5 :from :nyse?)))))
  (testing "col-refs without :from only includes :col"
    (is (= #{:mass} (datajure.expr/col-refs #dt/e (cut :mass 5))))))

(deftest cut-from-basic
  (testing "NYSE-style: predicate selects reference population for breakpoints"
    (core/reset-notes!)
    ;; exchcd=1 rows: indices 0,1,3 -> mktcap [5 15 35] -> 50th percentile breakpoint = 15
    ;; all mktcap [5 15 25 35 45] split by break at 15 -> bins [1 1 2 2 2]
    (let [data (ds/->dataset {:mktcap [5 15 25 35 45]
                              :exchcd [1 1 2 1 2]})
          result (core/dt data :set {:q #dt/e (cut :mktcap 2 :from (= :exchcd 1))})
          qs (vec (:q result))]
      (is (= [1 1 2 2 2] qs))))
  (testing "boolean column as :from selector (pre-computed flag)"
    (core/reset-notes!)
    ;; nyse? true rows: indices 0,1,3 -> mktcap [5 15 35] -> 50th percentile = 15
    ;; all mktcap [5 15 25 35 45] split by break at 15 -> bins [1 1 2 2 2]
    (let [data (ds/->dataset {:mktcap [5 15 25 35 45]
                              :nyse? [true true false true false]})
          result (core/dt data :set {:q #dt/e (cut :mktcap 2 :from :nyse?)})
          qs (vec (:q result))]
      (is (= [1 1 2 2 2] qs))))
  (testing "without :from is identical to :from with the same column"
    (core/reset-notes!)
    (let [data (ds/->dataset {:x [10 20 30 40 50 60 70 80]})
          r-without (core/dt data :set {:q #dt/e (cut :x 2)})
          r-with (core/dt data :set {:q #dt/e (cut :x 2 :from (>= :x 0))})]
      (is (= (vec (:q r-without)) (vec (:q r-with)))))))

(deftest cut-from-nil-handling
  (testing "false predicate rows excluded from breakpoint population"
    (core/reset-notes!)
    ;; exchcd=1 rows: mktcap [10 30 50] -> median breakpoint = 30
    ;; all mktcap [5 10 25 30 50 60 99] split by break at 30 -> [1 1 1 1 2 2 2]
    (let [data (ds/->dataset {:mktcap [5 10 25 30 50 60 99]
                              :exchcd [2 1 2 1 1 2 2]})
          result (core/dt data :set {:q #dt/e (cut :mktcap 2 :from (= :exchcd 1))})
          qs (vec (:q result))]
      (is (= [1 1 1 1 2 2 2] qs))))
  (testing "nils in :col still produce nil bins with :from predicate"
    (core/reset-notes!)
    (let [data (ds/->dataset {:mktcap [10 nil 30 nil 50]
                              :exchcd [1 1 1 1 1]})
          result (core/dt data :set {:q #dt/e (cut :mktcap 2 :from (= :exchcd 1))})
          qs (vec (:q result))]
      (is (nil? (nth qs 1)))
      (is (nil? (nth qs 3)))
      (is (some? (nth qs 0)))
      (is (some? (nth qs 2)))
      (is (some? (nth qs 4))))))

(deftest cut-from-column-validation
  (testing "unknown column in :from predicate throws :unknown-column"
    (let [data (ds/->dataset {:x [1 2 3]})]
      (is (thrown? clojure.lang.ExceptionInfo
                   (core/dt data :set {:q #dt/e (cut :x 4 :from (= :zzz 1))}))))))

(deftest qtile-basic
  (testing "qtile in :by produces equal-count bins with column-named result"
    (core/reset-notes!)
    (let [data (ds/->dataset {:species [:A :A :A :B :B :B :C :C :C :D :D :D]
                              :mass [3000 4000 5000 3500 4500 5500 3200 4200 5200 3800 4800 5800]})
          result (core/dt data :by [(core/qtile :mass 4)] :agg {:n core/N})]
      (is (= 4 (ds/row-count result)))
      (is (contains? (set (ds/column-names result)) :mass-q4))
      ;; 12 rows / 4 bins = 3 per bin
      (is (every? #(= 3 %) (vec (:n result))))))
  (testing "qtile is equivalent to #dt/e (cut :col n) on the same column"
    (core/reset-notes!)
    (let [data (ds/->dataset {:mass [3000 4000 5000 3500 4500 5500 3200 4200 5200 3800 4800 5800]})
          via-cut (-> (core/dt data :set {:q #dt/e (cut :mass 4)})
                      (core/dt :by [:q] :agg {:n core/N}))
          via-qtile (core/dt data :by [(core/qtile :mass 4)] :agg {:n core/N})]
      (is (= (vec (:n via-cut)) (vec (:n via-qtile)))))))

(deftest qtile-combined-with-keyword
  (testing "qtile combined with an exact key in :by"
    (core/reset-notes!)
    (let [data (ds/->dataset {:species [:A :A :A :B :B :B :C :C :C :D :D :D]
                              :mass [3000 4000 5000 3500 4500 5500 3200 4200 5200 3800 4800 5800]})
          result (core/dt data :by [:species (core/qtile :mass 2)] :agg {:n core/N})]
      ;; 4 species × 2 bins, but bin counts can be uneven within each species
      (is (= #{:species :mass-q2 :n} (set (ds/column-names result))))
      ;; every species appears with at least one bin assignment
      (is (= #{:A :B :C :D} (set (:species result)))))))

(deftest qtile-nil-handling
  (testing "nil values get their own group (nil key), non-nil values bin normally"
    (core/reset-notes!)
    (let [data (ds/->dataset {:mass [100 200 nil 400 nil 600 700]})
          result (core/dt data :by [(core/qtile :mass 3)] :agg {:n core/N})
          rows (ds/mapseq-reader result)
          nil-row (first (filter #(nil? (:mass-q3 %)) rows))]
      ;; 2 nil values form their own group
      (is (= 2 (:n nil-row)))
      ;; 5 non-nil values split into 3 bins
      (is (= 5 (reduce + (map :n (remove #(nil? (:mass-q3 %)) rows))))))))

(deftest qtile-unknown-column-error
  (testing "qtile with non-existent column throws :unknown-column at dispatch time"
    (core/reset-notes!)
    (let [data (ds/->dataset {:mass [1 2 3 4]})
          e (try (core/dt data :by [(core/qtile :flarg 2)] :agg {:n core/N}) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (some? e))
      (is (= :unknown-column (:dt/error (ex-data e))))
      (is (= #{:flarg} (:dt/columns (ex-data e)))))))

(deftest qtile-invalid-arguments
  (testing "qtile with non-positive n throws :qtile-invalid-n at call time"
    (let [e (try (core/qtile :mass 0) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :qtile-invalid-n (:dt/error (ex-data e))))))
  (testing "qtile with non-integer n throws :qtile-invalid-n"
    (let [e (try (core/qtile :mass 2.5) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :qtile-invalid-n (:dt/error (ex-data e))))))
  (testing "qtile with non-keyword column throws :qtile-invalid-col"
    (let [e (try (core/qtile "mass" 5) nil
                 (catch clojure.lang.ExceptionInfo e e))]
      (is (= :qtile-invalid-col (:dt/error (ex-data e)))))))

(deftest qtile-default-result-column-name
  (testing "default result column name is <col>-q<n>"
    (core/reset-notes!)
    (let [data (ds/->dataset {:score [10 20 30 40 50]})
          result (core/dt data :by [(core/qtile :score 5)] :agg {:n core/N})]
      (is (contains? (set (ds/column-names result)) :score-q5))))
  (testing "quintile bins of :mktcap produce :mktcap-q5"
    (core/reset-notes!)
    (let [data (ds/->dataset {:mktcap (range 1 21)})
          result (core/dt data :by [(core/qtile :mktcap 5)] :agg {:n core/N})]
      (is (contains? (set (ds/column-names result)) :mktcap-q5))
      ;; 20 rows / 5 bins = 4 per bin
      (is (every? #(= 4 %) (vec (:n result)))))))

(deftest core-full-name-agg-helpers
  (testing "mean skips nil"
    (let [col [3750.0 nil 3800.0 5000.0]]
      (is (= (core/mean col) (/ (+ 3750.0 3800.0 5000.0) 3)))))
  (testing "sum skips nil"
    (let [col [3750.0 nil 3800.0 5000.0]]
      (is (= (core/sum col) 12550.0))))
  (testing "median returns a number"
    (let [col [3750.0 3800.0 5000.0]]
      (is (number? (core/median col)))))
  (testing "stddev returns a number"
    (let [col [3750.0 3800.0 5000.0]]
      (is (number? (core/stddev col)))))
  (testing "variance returns a number"
    (let [col [3750.0 3800.0 5000.0]]
      (is (number? (core/variance col)))))
  (testing "max* returns column maximum, skips nil"
    (let [col [3750.0 nil 3800.0 5000.0]]
      (is (= 5000.0 (core/max* col)))))
  (testing "min* returns column minimum, skips nil"
    (let [col [3750.0 nil 3800.0 5000.0]]
      (is (= 3750.0 (core/min* col)))))
  (testing "count* counts non-nil values"
    (let [col [3750.0 nil 3800.0 nil 5000.0]]
      (is (= 3 (core/count* col)))))
  (testing "count* with no nils equals total count"
    (let [col [1.0 2.0 3.0]]
      (is (= 3 (core/count* col)))))
  (testing "count* with all nils returns 0"
    (let [col [nil nil nil]]
      (is (= 0 (core/count* col))))))


