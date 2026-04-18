(ns datajure.asof-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [datajure.join :refer [join]]
            [datajure.core :as core]))

;;; ---- fixtures --------------------------------------------------------------

(def trades
  (ds/->dataset {:sym ["A" "A" "B" "A"]
                 :time [1 3 2 6]}))

(def quotes
  (ds/->dataset {:sym ["A" "A" "A" "B"]
                 :time [1 2 5 1]
                 :bid [10 11 12 20]}))

;;; ---- basic asof ------------------------------------------------------------

(deftest asof-basic-test
  (testing "each trade matched to last quote with sym+time <= trade time"
    (let [result (join trades quotes :on [:sym :time] :how :asof)]
      (is (= 4 (ds/row-count result)))
      (is (= ["A" "A" "B" "A"] (vec (result :sym))))
      (is (= [1 3 2 6] (vec (result :time))))
      (is (= [10 11 20 12] (vec (result :bid)))))))

(deftest asof-column-set-test
  (testing "result has exactly left cols + right non-key cols"
    (let [result (join trades quotes :on [:sym :time] :how :asof)]
      (is (= #{:sym :time :bid} (set (ds/column-names result)))))))

;;; ---- unmatched left rows ---------------------------------------------------

(deftest asof-unmatched-no-exact-key-test
  (testing "left rows whose exact key has no right group → nil right cols"
    (let [left (ds/->dataset {:sym ["A" "C"] :time [1 1]})
          result (join left quotes :on [:sym :time] :how :asof)]
      (is (= 2 (ds/row-count result)))
      (is (= [10 nil] (vec (result :bid)))))))

(deftest asof-unmatched-asof-too-early-test
  (testing "left asof-key earlier than all right asof-keys for that group → nil"
    (let [left (ds/->dataset {:sym ["A"] :time [0]})
          result (join left quotes :on [:sym :time] :how :asof)]
      (is (= 1 (ds/row-count result)))
      (is (nil? (first (result :bid)))))))

(deftest asof-all-unmatched-test
  (testing "all left rows unmatched → full-width result with nil right cols"
    (let [left (ds/->dataset {:sym ["Z" "Z"] :time [1 2]})
          result (join left quotes :on [:sym :time] :how :asof)]
      (is (= 2 (ds/row-count result)))
      (is (every? nil? (result :bid))))))

;;; ---- nil values in right asof column --------------------------------------

(deftest asof-nil-in-right-asof-col-test
  (testing "right rows with nil asof-val are skipped; real matches still found"
    ;; right has time=[nil 1 nil] — after sort: [1 nil nil]
    ;; left time=1 should match right row with time=1, bid=11
    (let [right (ds/->dataset {:sym ["A" "A" "A"] :time [nil 1 nil] :bid [10 11 12]})
          left (ds/->dataset {:sym ["A"] :time [1]})
          result (join left right :on [:sym :time] :how :asof)]
      (is (= 1 (ds/row-count result)))
      (is (= [11] (vec (result :bid)))))))

;;; ---- asymmetric key names --------------------------------------------------

(deftest asof-left-on-right-on-test
  (testing ":left-on / :right-on with different column names"
    (let [left (ds/->dataset {:sym ["A" "A"] :trade-time [2 4]})
          right (ds/->dataset {:sym ["A" "A" "A"] :quote-time [1 3 5] :bid [10 11 12]})
          result (join left right
                       :left-on [:sym :trade-time]
                       :right-on [:sym :quote-time]
                       :how :asof)]
      (is (= 2 (ds/row-count result)))
      (is (= [10 11] (vec (result :bid))))
      (is (= #{:sym :trade-time :bid} (set (ds/column-names result)))))))

;;; ---- single asof key (no exact cols) ---------------------------------------

(deftest asof-single-key-test
  (testing "single :on col means asof-only, no exact grouping"
    (let [left (ds/->dataset {:time [2 4 6] :x [1 2 3]})
          right (ds/->dataset {:time [1 3 5] :val [10 20 30]})
          result (join left right :on [:time] :how :asof)]
      (is (= 3 (ds/row-count result)))
      (is (= [10 20 30] (vec (result :val)))))))

;;; ---- conflicting non-key column names --------------------------------------

(deftest asof-conflicting-col-names-test
  (testing "non-key col present in both sides gets :right. suffix"
    (let [left (ds/->dataset {:sym ["A"] :time [2] :val [99]})
          right (ds/->dataset {:sym ["A"] :time [1] :val [10] :ask [1.0]})
          result (join left right :on [:sym :time] :how :asof)]
      (is (contains? (set (ds/column-names result)) :right.val))
      (is (contains? (set (ds/column-names result)) :val))
      (is (contains? (set (ds/column-names result)) :ask)))))

;;; ---- :validate -------------------------------------------------------------

(deftest asof-validate-m-1-pass-test
  (testing ":validate :m:1 passes when right keys are unique"
    (let [result (join trades quotes :on [:sym :time] :how :asof :validate :m:1)]
      (is (= 4 (ds/row-count result))))))

(deftest asof-validate-m-1-fail-test
  (testing ":validate :m:1 throws when right has duplicate keys"
    (let [duped-r (ds/->dataset {:sym ["A" "A"] :time [1 1] :bid [10 99]})]
      (is (thrown-with-msg? Exception #"right dataset has duplicate keys"
                            (join trades duped-r :on [:sym :time]
                                  :how :asof :validate :m:1))))))

(deftest asof-validate-unknown-test
  (testing "unknown :validate value throws"
    (is (thrown-with-msg? Exception #"Unknown :validate value"
                          (join trades quotes :on [:sym :time]
                                :how :asof :validate :foo)))))

(deftest asof-validate-ex-data-test
  (testing "cardinality violation has structured ex-data"
    (let [duped-r (ds/->dataset {:sym ["A" "A"] :time [1 1] :bid [10 99]})]
      (try
        (join trades duped-r :on [:sym :time] :how :asof :validate :m:1)
        (catch Exception e
          (let [d (ex-data e)]
            (is (= :join-cardinality-violation (:dt/error d)))
            (is (= :m:1 (:dt/validate d)))
            (is (= :right (:dt/side d)))))))))

;;; ---- error paths -----------------------------------------------------------

(deftest asof-error-on-with-left-on-test
  (testing "combining :on with :left-on/:right-on still errors for :asof"
    (is (thrown-with-msg? Exception #"Cannot combine :on with :left-on"
                          (join trades quotes :on [:sym :time]
                                :left-on [:sym :time] :how :asof)))))

(deftest asof-error-missing-keys-test
  (testing "missing key spec errors for :asof"
    (is (thrown-with-msg? Exception #"Must provide either :on"
                          (join trades quotes :how :asof)))))

(deftest asof-unknown-how-still-errors-test
  (testing ":how :bogus still errors after adding :asof"
    (is (thrown-with-msg? Exception #"Unknown join type"
                          (join trades quotes :on [:sym :time] :how :bogus)))))

;;; ---- pipeline integration --------------------------------------------------

(deftest asof-pipeline-with-dt-test
  (testing "asof join result threads into dt"
    (let [result (-> (join trades quotes :on [:sym :time] :how :asof)
                     (core/dt :where #dt/e (> :bid 10)
                              :order-by [(core/asc :bid)]))]
      (is (= 3 (ds/row-count result)))
      (is (= [11 12 20] (vec (result :bid)))))))

(deftest asof-ohlc-pattern-test
  (testing "OHLC-style: match each bar to last prevailing quote"
    (let [bars (ds/->dataset {:sym ["A" "A" "A"]
                              :time [2 4 6]})
          ticks (ds/->dataset {:sym ["A" "A" "A" "A" "A"]
                               :time [1 2 3 4 5]
                               :px [10 11 12 13 14]})
          result (join bars ticks :on [:sym :time] :how :asof)]
      (is (= 3 (ds/row-count result)))
      (is (= [11 13 14] (vec (result :px)))))))

(deftest asof-report-test
  (testing ":report true on asof join prints diagnostics and does not suppress result"
    (let [output (with-out-str
                   (join trades quotes :on [:sym :time] :how :asof :report true))]
      (is (re-find #"\[datajure\] join report:" output))))
  (testing ":report false on asof join prints nothing"
    (let [output (with-out-str
                   (join trades quotes :on [:sym :time] :how :asof :report false))]
      (is (= "" output))))
  (testing ":report true on asof join still returns correct dataset"
    (let [result (join trades quotes :on [:sym :time] :how :asof :report true)]
      (is (ds/dataset? result))
      (is (= 4 (ds/row-count result))))))

;;; ---- regression: regular joins unaffected ----------------------------------

(deftest regular-joins-unaffected-test
  (testing "adding :asof does not break regular join dispatch"
    (let [l (ds/->dataset {:id [1 2 3] :x ["a" "b" "c"]})
          r (ds/->dataset {:id [2 3 4] :y [20 30 40]})]
      (is (= 2 (ds/row-count (join l r :on :id :how :inner))))
      (is (= 3 (ds/row-count (join l r :on :id :how :left))))
      (is (= 3 (ds/row-count (join l r :on :id :how :right))))
      (is (= 4 (ds/row-count (join l r :on :id :how :outer)))))))

(deftest asof-forward-basic-test
  (testing ":direction :forward matches first right row where right >= left"
    (let [left (ds/->dataset {:sym ["A" "A" "A"] :time [2 4 6]})
          right (ds/->dataset {:sym ["A" "A" "A"] :time [1 3 5] :bid [10 20 30]})
          result (join left right :on [:sym :time] :how :asof :direction :forward)]
      (is (= 3 (ds/row-count result)))
      (is (= [20 30 nil] (vec (:bid result))))))
  (testing "forward: left earlier than all right -> match first right"
    (let [left (ds/->dataset {:sym ["A"] :time [0]})
          right (ds/->dataset {:sym ["A" "A"] :time [1 2] :bid [10 20]})
          result (join left right :on [:sym :time] :how :asof :direction :forward)]
      (is (= [10] (vec (:bid result))))))
  (testing "forward: left later than all right -> nil"
    (let [left (ds/->dataset {:sym ["A"] :time [9]})
          right (ds/->dataset {:sym ["A" "A"] :time [1 5] :bid [10 20]})
          result (join left right :on [:sym :time] :how :asof :direction :forward)]
      (is (nil? (first (:bid result)))))))

(deftest asof-nearest-basic-test
  (testing ":direction :nearest picks the closer of backward and forward"
    (let [left (ds/->dataset {:sym ["A" "A" "A"] :time [3 5 6]})
          right (ds/->dataset {:sym ["A" "A" "A"] :time [1 4 7] :bid [10 20 30]})
          result (join left right :on [:sym :time] :how :asof :direction :nearest)]
      ;; left=3: bwd=1(dist=2) fwd=4(dist=1) -> fwd=4, bid=20
      ;; left=5: bwd=4(dist=1) fwd=7(dist=2) -> bwd=4, bid=20
      ;; left=6: bwd=4(dist=2) fwd=7(dist=1) -> fwd=7, bid=30
      (is (= [20 20 30] (vec (:bid result))))))
  (testing ":nearest tie (equidistant) prefers backward"
    (let [left (ds/->dataset {:sym ["A"] :time [3]})
          right (ds/->dataset {:sym ["A" "A"] :time [1 5] :bid [10 20]})
          result (join left right :on [:sym :time] :how :asof :direction :nearest)]
      ;; bwd=1(dist=2) fwd=5(dist=2) -> tie -> backward -> bid=10
      (is (= [10] (vec (:bid result))))))
  (testing ":nearest with no forward match falls back to backward"
    (let [left (ds/->dataset {:sym ["A"] :time [9]})
          right (ds/->dataset {:sym ["A" "A"] :time [1 5] :bid [10 20]})
          result (join left right :on [:sym :time] :how :asof :direction :nearest)]
      (is (= [20] (vec (:bid result))))))
  (testing ":nearest with no backward match falls back to forward"
    (let [left (ds/->dataset {:sym ["A"] :time [0]})
          right (ds/->dataset {:sym ["A" "A"] :time [1 5] :bid [10 20]})
          result (join left right :on [:sym :time] :how :asof :direction :nearest)]
      (is (= [10] (vec (:bid result)))))))

(deftest asof-tolerance-backward-test
  (testing ":tolerance cuts off backward matches beyond max distance"
    (let [left (ds/->dataset {:time [3 7 15]})
          right (ds/->dataset {:time [1 5] :bid [10 20]})
          ;; left=3:  bwd=1(dist=2) <= 3 -> bid=10
          ;; left=7:  bwd=5(dist=2) <= 3 -> bid=20
          ;; left=15: bwd=5(dist=10) > 3 -> nil
          result (join left right :on [:time] :how :asof :tolerance 3)]
      (is (= [10 20 nil] (vec (:bid result))))))
  (testing ":tolerance=0 only matches exact values"
    (let [left (ds/->dataset {:time [1 2 3]})
          right (ds/->dataset {:time [1 3] :bid [10 30]})
          result (join left right :on [:time] :how :asof :tolerance 0)]
      (is (= [10 nil 30] (vec (:bid result)))))))

(deftest asof-tolerance-forward-test
  (testing ":tolerance with :direction :forward"
    (let [left (ds/->dataset {:time [3 4 8]})
          right (ds/->dataset {:time [1 5] :bid [10 20]})
          ;; left=3: fwd=5(dist=2) <= 2 -> bid=20
          ;; left=4: fwd=5(dist=1) <= 2 -> bid=20
          ;; left=8: no fwd match -> nil
          result (join left right :on [:time] :how :asof
                       :direction :forward :tolerance 2)]
      (is (= [20 20 nil] (vec (:bid result)))))))

(deftest asof-tolerance-nearest-test
  (testing ":tolerance with :direction :nearest"
    (let [left (ds/->dataset {:time [3 4]})
          right (ds/->dataset {:time [1 5] :bid [10 20]})
          ;; left=3: nearest is bwd=1(dist=2) or fwd=5(dist=2) -> tie -> bwd=1 -> dist=2 > 1 -> nil
          ;; left=4: nearest is fwd=5(dist=1) -> dist=1 <= 1 -> bid=20
          result (join left right :on [:time] :how :asof
                       :direction :nearest :tolerance 1)]
      (is (= [nil 20] (vec (:bid result)))))))

(deftest asof-direction-unknown-error-test
  (testing "unknown :direction throws :join-unknown-direction"
    (let [e (try
              (join trades quotes :on [:sym :time] :how :asof :direction :sideways)
              nil
              (catch Exception e e))]
      (is (some? e))
      (is (= :join-unknown-direction (:dt/error (ex-data e))))
      (is (= :sideways (:dt/direction (ex-data e)))))))
