(ns datajure.join-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.join :refer [join]]
            [datajure.core :as core]))

(def lhs (ds/->dataset {:id [1 2 3] :x ["a" "b" "c"]}))
(def rhs (ds/->dataset {:id [2 3 4] :y [20 30 40]}))

(deftest inner-join-test
  (let [result (join lhs rhs :on :id :how :inner)]
    (is (= 2 (ds/row-count result)))
    (is (= [2 3] (vec (result :id))))
    (is (= ["b" "c"] (vec (result :x))))
    (is (= [20 30] (vec (result :y))))))

(deftest inner-join-default-test
  (testing ":inner is the default :how"
    (let [result (join lhs rhs :on :id)]
      (is (= 2 (ds/row-count result))))))

(deftest left-join-test
  (let [result (join lhs rhs :on :id :how :left)]
    (is (= 3 (ds/row-count result)))
    (is (= #{1 2 3} (set (result :id))))))

(deftest right-join-test
  (let [result (join lhs rhs :on :id :how :right)]
    (is (= 3 (ds/row-count result)))
    (is (= #{2 3 4} (set (result :id))))))

(deftest outer-join-test
  (let [result (join lhs rhs :on :id :how :outer)]
    (is (= 4 (ds/row-count result)))
    (is (= #{1 2 3 4} (set (result :id))))))

(deftest multi-column-on-test
  (let [l (ds/->dataset {:a [1 1 2] :b ["x" "y" "x"] :val [10 20 30]})
        r (ds/->dataset {:a [1 2 2] :b ["x" "x" "y"] :score [100 200 300]})
        result (join l r :on [:a :b] :how :inner)]
    (is (= 2 (ds/row-count result)))
    (is (= [1 2] (vec (result :a))))
    (is (= ["x" "x"] (vec (result :b))))))

(deftest left-on-right-on-test
  (let [l (ds/->dataset {:id [1 2 3] :x ["a" "b" "c"]})
        r (ds/->dataset {:key [2 3 4] :y [20 30 40]})
        result (join l r :left-on :id :right-on :key :how :left)]
    (is (= 3 (ds/row-count result)))
    (is (= #{1 2 3} (set (result :id))))))

(deftest error-on-with-left-on-test
  (is (thrown-with-msg? Exception #"Cannot combine :on with :left-on"
                        (join lhs rhs :on :id :left-on :id))))

(deftest error-missing-keys-test
  (is (thrown-with-msg? Exception #"Must provide either :on"
                        (join lhs rhs :how :inner))))

(deftest error-unknown-how-test
  (is (thrown-with-msg? Exception #"Unknown join type"
                        (join lhs rhs :on :id :how :cross))))

(deftest composable-with-dt-test
  (let [result (-> (join lhs rhs :on :id :how :left)
                   (core/dt :where #dt/e (> :id 1))
                   (core/dt :select [:id :x :y]))]
    (is (= 2 (ds/row-count result)))
    (is (= [2 3] (vec (result :id))))))

(deftest validate-1-1-pass-test
  (testing ":1:1 passes when both sides have unique keys"
    (let [result (join lhs rhs :on :id :validate :1:1)]
      (is (= 2 (ds/row-count result))))))

(deftest validate-1-1-fail-left-test
  (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})]
    (is (thrown-with-msg? Exception #"left dataset has duplicate keys"
                          (join duped-l rhs :on :id :validate :1:1)))))

(deftest validate-1-1-fail-right-test
  (let [duped-r (ds/->dataset {:id [2 2 3] :y [20 30 40]})]
    (is (thrown-with-msg? Exception #"right dataset has duplicate keys"
                          (join lhs duped-r :on :id :validate :1:1)))))

(deftest validate-1-m-pass-test
  (testing ":1:m passes when left is unique"
    (let [duped-r (ds/->dataset {:id [2 2 3] :y [20 30 40]})
          result (join lhs duped-r :on :id :how :inner :validate :1:m)]
      (is (= 3 (ds/row-count result))))))

(deftest validate-1-m-fail-test
  (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})]
    (is (thrown-with-msg? Exception #"left dataset has duplicate keys"
                          (join duped-l rhs :on :id :validate :1:m)))))

(deftest validate-m-1-pass-test
  (testing ":m:1 passes when right is unique"
    (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})
          result (join duped-l rhs :on :id :how :inner :validate :m:1)]
      (is (= 1 (ds/row-count result))))))

(deftest validate-m-1-fail-test
  (let [duped-r (ds/->dataset {:id [2 2 3] :y [20 30 40]})]
    (is (thrown-with-msg? Exception #"right dataset has duplicate keys"
                          (join lhs duped-r :on :id :validate :m:1)))))

(deftest validate-m-m-pass-test
  (testing ":m:m always passes"
    (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})
          duped-r (ds/->dataset {:id [2 2 3] :y [20 30 40]})
          result (join duped-l duped-r :on :id :how :inner :validate :m:m)]
      (is (= 2 (ds/row-count result))))))

(deftest validate-unknown-value-test
  (is (thrown-with-msg? Exception #"Unknown :validate value"
                        (join lhs rhs :on :id :validate :foo))))

(deftest validate-with-left-on-right-on-test
  (testing ":validate works with asymmetric key names"
    (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})
          r (ds/->dataset {:key [2 3 4] :y [20 30 40]})]
      (is (thrown-with-msg? Exception #"left dataset has duplicate keys"
                            (join duped-l r :left-on :id :right-on :key :validate :1:1))))))

(deftest report-basic-test
  (testing ":report prints merge diagnostics"
    (let [output (with-out-str (join lhs rhs :on :id :how :left :report true))]
      (is (re-find #"2 matched" output))
      (is (re-find #"1 left-only" output))
      (is (re-find #"1 right-only" output)))))

(deftest report-full-overlap-test
  (testing ":report with full overlap shows 0 left-only and right-only"
    (let [r (ds/->dataset {:id [1 2 3] :y [10 20 30]})
          output (with-out-str (join lhs r :on :id :report true))]
      (is (re-find #"3 matched" output))
      (is (re-find #"0 left-only" output))
      (is (re-find #"0 right-only" output)))))

(deftest report-no-output-by-default-test
  (testing "no report output when :report is not set"
    (let [output (with-out-str (join lhs rhs :on :id))]
      (is (= "" output)))))

(deftest multi-column-left-join-test
  (let [l (ds/->dataset {:a [1 1 2] :b ["x" "y" "x"] :val [10 20 30]})
        r (ds/->dataset {:a [1 2 2] :b ["x" "x" "y"] :score [100 200 300]})
        result (join l r :on [:a :b] :how :left)]
    (is (= 3 (ds/row-count result)))
    (is (= #{[1 "x"] [1 "y"] [2 "x"]} (set (map vector (result :a) (result :b)))))))

(deftest no-overlap-join-test
  (let [l (ds/->dataset {:id [1 2] :x ["a" "b"]})
        r (ds/->dataset {:id [3 4] :y [30 40]})]
    (testing "inner join with no overlap returns empty"
      (is (= 0 (ds/row-count (join l r :on :id :how :inner)))))
    (testing "left join preserves all left rows"
      (is (= 2 (ds/row-count (join l r :on :id :how :left)))))
    (testing "report shows 0 matched"
      (let [output (with-out-str (join l r :on :id :report true))]
        (is (re-find #"0 matched" output))
        (is (re-find #"2 left-only" output))
        (is (re-find #"2 right-only" output))))))

(deftest validate-multi-column-on-test
  (testing ":validate with multi-column keys"
    (let [l (ds/->dataset {:a [1 1 2] :b ["x" "x" "y"] :val [10 20 30]})
          r (ds/->dataset {:a [1 2] :b ["x" "y"] :score [100 200]})]
      (is (thrown-with-msg? Exception #"left dataset has duplicate keys"
                            (join l r :on [:a :b] :validate :1:1))))))

(deftest validate-and-report-together-test
  (testing ":validate and :report work together"
    (let [output (with-out-str (join lhs rhs :on :id :how :left :validate :1:1 :report true))]
      (is (re-find #"2 matched" output)))))

(deftest report-with-left-on-right-on-test
  (testing ":report works with asymmetric key names"
    (let [r (ds/->dataset {:key [2 3 4] :y [20 30 40]})
          output (with-out-str (join lhs r :left-on :id :right-on :key :how :left :report true))]
      (is (re-find #"2 matched" output))
      (is (re-find #"1 left-only" output)))))

(deftest ex-data-structure-test
  (testing "cardinality error has structured ex-data"
    (let [duped-l (ds/->dataset {:id [1 1 2] :x ["a" "b" "c"]})]
      (try (join duped-l rhs :on :id :validate :1:1)
           (catch Exception e
             (let [d (ex-data e)]
               (is (= :join-cardinality-violation (:dt/error d)))
               (is (= :1:1 (:dt/validate d)))
               (is (= :left (:dt/side d)))
               (is (= [:id] (:dt/keys d)))))))))

(deftest full-pipeline-with-join-test
  (testing "join → dt pipeline matching spec examples"
    (let [result (-> (join lhs rhs :on :id :how :left)
                     (core/dt :where #dt/e (> :y 0)
                              :set {:y2 #dt/e (* :y 2)})
                     (core/dt :order-by [(core/desc :y2)])
                     (core/dt :select [:id :y2]))]
      (is (= 2 (ds/row-count result)))
      (is (= [60 40] (vec (result :y2)))))))

(deftest wjoin-basic-test
  (testing "backward window [-2 0]: aggregates right rows in [left-t - 2, left-t]"
    ;; right: times 1..8, bids 10..17
    ;; left=2: window [0,2] -> times [1,2] -> bids [10,11] -> mean 10.5
    ;; left=5: window [3,5] -> times [3,4,5] -> bids [12,13,14] -> mean 13.0
    ;; left=8: window [6,8] -> times [6,7,8] -> bids [15,16,17] -> mean 16.0
    (let [right (ds/->dataset {:time (range 1 9) :bid (map double (range 10 18))})
          left (ds/->dataset {:time [2 5 8]})
          result (join left right :on [:time] :how :window
                       :window [-2 0]
                       :agg {:mean-bid #dt/e (mn :bid)})]
      (is (= 3 (ds/row-count result)))
      (is (= [10.5 13.0 16.0] (vec (:mean-bid result)))))))

(deftest wjoin-nrow-agg-test
  (testing "plain fn agg (nrow) counts matched rows; 0 for empty window"
    (let [right (ds/->dataset {:time (range 1 9) :bid (map double (range 10 18))})
          left (ds/->dataset {:time [2 5 0]})
          ;; left=0: window [-2,0], no right times <= 0 -> empty -> nrow=0
          result (join left right :on [:time] :how :window
                       :window [-2 0]
                       :agg {:n core/nrow})]
      (is (= [2 3 0] (vec (:n result)))))))

(deftest wjoin-empty-window-nil-test
  (testing "#dt/e agg returns nil for empty window; plain fn returns natural result"
    (let [right (ds/->dataset {:time [5 6 7] :bid [10.0 20.0 30.0]})
          left (ds/->dataset {:time [2]}) ;; window [0,2] -> no right rows
          result (join left right :on [:time] :how :window
                       :window [-2 0]
                       :agg {:mean-bid #dt/e (mn :bid) :n core/nrow})]
      (is (nil? (first (:mean-bid result))))
      (is (= 0 (first (:n result)))))))

(deftest wjoin-no-exact-key-test
  (testing "left exact-key not in right -> nil for all agg cols"
    (let [right (ds/->dataset {:sym ["A"] :time [1] :bid [10.0]})
          left (ds/->dataset {:sym ["B"] :time [1]})
          result (join left right :on [:sym :time] :how :window
                       :window [-1 0]
                       :agg {:mean-bid #dt/e (mn :bid) :n core/nrow})]
      (is (nil? (first (:mean-bid result))))
      (is (= 0 (first (:n result)))))))

(deftest wjoin-forward-window-test
  (testing "forward window [0 3]: right rows in [left-t, left-t + 3]"
    ;; right: times 1..5, bids 10..50
    ;; left=2: window [2,5] -> times [2,3,4,5] -> bids [20,30,40,50] -> mean 35.0
    (let [right (ds/->dataset {:time (range 1 6) :bid (map #(* 10.0 %) (range 1 6))})
          left (ds/->dataset {:time [2]})
          result (join left right :on [:time] :how :window
                       :window [0 3]
                       :agg {:mean-bid #dt/e (mn :bid)})]
      (is (= [35.0] (vec (:mean-bid result)))))))

(deftest wjoin-multi-agg-test
  (testing "multiple agg columns computed in one call"
    (let [right (ds/->dataset {:time [1 2 3 4 5]
                               :bid [10.0 20.0 30.0 40.0 50.0]
                               :ask [11.0 21.0 31.0 41.0 51.0]})
          left (ds/->dataset {:time [3]})
          ;; window [-2 0] -> times [1,2,3] -> bids [10,20,30] -> mean 20; asks [11,21,31] -> mean 21
          result (join left right :on [:time] :how :window
                       :window [-2 0]
                       :agg {:mean-bid #dt/e (mn :bid) :mean-ask #dt/e (mn :ask)})]
      (is (= [20.0] (vec (:mean-bid result))))
      (is (= [21.0] (vec (:mean-ask result)))))))

(deftest wjoin-multi-key-test
  (testing "exact-key (sym) + asof-key (time) — partitioned window"
    (let [right (ds/->dataset {:sym ["A" "A" "A" "B" "B"]
                               :time [1 2 3 1 2]
                               :bid [10.0 20.0 30.0 100.0 200.0]})
          left (ds/->dataset {:sym ["A" "B"] :time [2 2]})
          ;; A/2: window [1,2] -> times [1,2] -> bids [10,20] -> mean 15
          ;; B/2: window [1,2] -> times [1,2] -> bids [100,200] -> mean 150
          result (join left right :on [:sym :time] :how :window
                       :window [-1 0]
                       :agg {:mean-bid #dt/e (mn :bid)})]
      (is (= 2 (ds/row-count result)))
      (is (= [15.0 150.0] (vec (:mean-bid result)))))))

(deftest wjoin-unit-minutes-test
  (testing "[:lo :hi :minutes] converts offsets to milliseconds"
    ;; right: epoch-ms 0, 60000 (1min), 120000 (2min)
    ;; left: 120000 ms (2min), window [-1 0 :minutes] -> [60000, 120000]
    ;; matched: [60000, 120000] -> bids [20, 30] -> mean 25
    (let [right (ds/->dataset {:time [0 60000 120000] :bid [10.0 20.0 30.0]})
          left (ds/->dataset {:time [120000]})
          result (join left right :on [:time] :how :window
                       :window [-1 0 :minutes]
                       :agg {:mean-bid #dt/e (mn :bid)})]
      (is (= [25.0] (vec (:mean-bid result))))))
  (testing "[lo unit hi] ordering also works"
    (let [right (ds/->dataset {:time [0 60000 120000] :bid [10.0 20.0 30.0]})
          left (ds/->dataset {:time [120000]})
          result (join left right :on [:time] :how :window
                       :window [-1 :minutes 0]
                       :agg {:mean-bid #dt/e (mn :bid)})]
      (is (= [25.0] (vec (:mean-bid result)))))))

(deftest wjoin-left-on-right-on-test
  (testing ":left-on/:right-on work with window join"
    (let [right (ds/->dataset {:t [1 2 3 4 5] :bid [10.0 20.0 30.0 40.0 50.0]})
          left (ds/->dataset {:ts [3]})
          result (join left right :left-on [:ts] :right-on [:t]
                       :how :window :window [-1 0]
                       :agg {:mean-bid #dt/e (mn :bid)})]
      ;; window [2,3] -> t=[2,3] -> bids=[20,30] -> mean 25
      (is (= [25.0] (vec (:mean-bid result)))))))

(deftest wjoin-missing-window-error-test
  (testing "no :window spec throws :join-missing-window"
    (let [e (try (join (ds/->dataset {:time [1]}) (ds/->dataset {:time [1] :bid [1.0]})
                       :on [:time] :how :window :agg {:n core/nrow})
                 nil (catch Exception e e))]
      (is (= :join-missing-window (:dt/error (ex-data e)))))))

(deftest wjoin-missing-agg-error-test
  (testing "no :agg throws :join-missing-agg"
    (let [e (try (join (ds/->dataset {:time [1]}) (ds/->dataset {:time [1] :bid [1.0]})
                       :on [:time] :how :window :window [-1 0])
                 nil (catch Exception e e))]
      (is (= :join-missing-agg (:dt/error (ex-data e)))))))

(deftest wjoin-unknown-unit-error-test
  (testing "unknown unit throws :join-unknown-window-unit"
    (let [e (try (join (ds/->dataset {:time [1]}) (ds/->dataset {:time [1] :bid [1.0]})
                       :on [:time] :how :window :window [-1 0 :nanoseconds] :agg {:n core/nrow})
                 nil (catch Exception e e))]
      (is (= :join-unknown-window-unit (:dt/error (ex-data e))))
      (is (= :nanoseconds (:unit (ex-data e)))))))

(deftest wjoin-invalid-window-shape-test
  ;; Phase 63: parse-window-spec used to destructure [a b c :as wspec] and
  ;; silently drop trailing elements. Malformed shapes now throw a structured
  ;; :join-invalid-window error so bad specs fail fast.
  (let [left (ds/->dataset {:time [1]})
        right (ds/->dataset {:time [1] :bid [1.0]})
        try-join (fn [spec]
                   (try (join left right :on [:time] :how :window
                              :window spec :agg {:n core/nrow})
                        nil
                        (catch clojure.lang.ExceptionInfo e (ex-data e))))]
    (testing "trailing junk after valid [lo hi unit] is rejected (not silently dropped)"
      (let [ed (try-join [-5 0 :minutes :junk])]
        (is (= :join-invalid-window (:dt/error ed)))
        (is (= [-5 0 :minutes :junk] (:dt/window ed)))))
    (testing "too few elements"
      (is (= :join-invalid-window (:dt/error (try-join []))))
      (is (= :join-invalid-window (:dt/error (try-join [5])))))
    (testing "four elements without a keyword in pos 2 or 3 is still a shape error"
      (is (= :join-invalid-window (:dt/error (try-join [-5 0 10 20])))))
    (testing "non-numeric endpoint in [lo hi]"
      (is (= :join-invalid-window (:dt/error (try-join ["-5" 0])))))
    (testing "non-numeric endpoint in [lo hi unit]"
      (is (= :join-invalid-window (:dt/error (try-join ["-5" 0 :minutes])))))
    (testing "non-numeric endpoint in [lo unit hi]"
      (is (= :join-invalid-window (:dt/error (try-join [-5 :minutes "0"])))))
    (testing "non-vector window spec"
      (is (= :join-invalid-window (:dt/error (try-join :not-a-vector)))))
    (testing "regression: valid two-element spec still works"
      (let [result (join left right :on [:time] :how :window
                         :window [-1 0] :agg {:n core/nrow})]
        (is (= 1 (ds/row-count result)))))
    (testing "regression: valid three-element [lo hi unit] still works"
      (let [result (join left right :on [:time] :how :window
                         :window [-1 0 :seconds] :agg {:n core/nrow})]
        (is (= 1 (ds/row-count result)))))
    (testing "regression: valid three-element [lo unit hi] still works"
      (let [result (join left right :on [:time] :how :window
                         :window [-1 :seconds 0] :agg {:n core/nrow})]
        (is (= 1 (ds/row-count result)))))))

(deftest wjoin-pipeline-test
  (testing "window join result threads naturally into dt"
    (let [right (ds/->dataset {:sym ["A" "A" "A" "B" "B" "B"]
                               :time [1 2 3 1 2 3]
                               :bid [10.0 20.0 30.0 100.0 200.0 300.0]})
          left (ds/->dataset {:sym ["A" "B"] :time [3 3]})
          result (-> (join left right :on [:sym :time] :how :window
                           :window [-2 0]
                           :agg {:mean-bid #dt/e (mn :bid) :n core/nrow})
                     (core/dt :order-by [(core/asc :sym)]))]
      (is (= 2 (ds/row-count result)))
      (is (= ["A" "B"] (vec (:sym result))))
      (is (= [20.0 200.0] (vec (:mean-bid result))))
      (is (= [3 3] (vec (:n result)))))))
