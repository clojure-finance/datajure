(ns datajure.nrepl-test
  (:require [clojure.test :refer [deftest is testing]]
            [tech.v3.dataset :as ds]
            [nrepl.transport :as t]
            [datajure.core :as core]
            [datajure.nrepl :as nrepl]))

(def ^:private test-ds
  (ds/->dataset {:a [1 2 3] :b [4 5 6]}))

(defn- make-recording-transport []
  (let [sent (atom [])]
    {:transport (reify t/Transport
                  (recv [_] nil)
                  (recv [_ _] nil)
                  (send [_ response] (swap! sent conj response)))
     :sent sent}))

(deftest dt-var-is-dynamic
  (is (.isDynamic #'core/*dt*))
  (is (nil? core/*dt*)))

(deftest dataset-detection
  (testing "recognises tech.v3.dataset"
    (is (#'nrepl/dataset? test-ds)))
  (testing "rejects non-datasets"
    (is (not (#'nrepl/dataset? 42)))
    (is (not (#'nrepl/dataset? {:a 1})))
    (is (not (#'nrepl/dataset? nil)))
    (is (not (#'nrepl/dataset? [1 2 3])))))

(deftest wrapping-transport-binds-dt-on-dataset-result
  (testing "dataset *1 sets *dt*"
    (let [{:keys [transport]} (make-recording-transport)
          session (atom {#'core/*dt* nil})
          wt (#'nrepl/wrapping-transport transport session)]
      (binding [*1 test-ds
                core/*dt* nil]
        (t/send wt {:value "(ds ...)" :ns "user"})
        (is (= test-ds core/*dt*))))))

(deftest wrapping-transport-ignores-non-dataset
  (testing "non-dataset *1 does not change *dt*"
    (let [{:keys [transport]} (make-recording-transport)
          session (atom {#'core/*dt* nil})
          wt (#'nrepl/wrapping-transport transport session)]
      (binding [*1 42
                core/*dt* nil]
        (t/send wt {:value "42" :ns "user"})
        (is (nil? core/*dt*))))))

(deftest wrapping-transport-ignores-non-value-response
  (testing "response without :value key does not touch *dt*"
    (let [{:keys [transport]} (make-recording-transport)
          session (atom {#'core/*dt* nil})
          wt (#'nrepl/wrapping-transport transport session)]
      (binding [*1 test-ds
                core/*dt* nil]
        (t/send wt {:status ["done"]})
        (is (nil? core/*dt*))))))

(deftest wrapping-transport-forwards-response
  (testing "response is forwarded to inner transport"
    (let [{:keys [transport sent]} (make-recording-transport)
          session (atom {#'core/*dt* nil})
          wt (#'nrepl/wrapping-transport transport session)]
      (binding [*1 test-ds
                core/*dt* nil]
        (t/send wt {:value "(ds ...)" :ns "user"})
        (is (= 1 (count @sent)))
        (is (contains? (first @sent) :value))))))

(deftest wrap-dt-initialises-session
  (testing "adds *dt* to session if absent"
    (let [session (atom {})
          handler (fn [_] :ok)
          wrapped (nrepl/wrap-dt handler)]
      (wrapped {:op "eval" :session session})
      (is (contains? @session #'core/*dt*))))
  (testing "does not overwrite existing *dt* in session"
    (let [session (atom {#'core/*dt* test-ds})
          handler (fn [_] :ok)
          wrapped (nrepl/wrap-dt handler)]
      (wrapped {:op "eval" :session session})
      (is (= test-ds (get @session #'core/*dt*))))))

(deftest wrap-dt-wraps-eval-only
  (testing "eval ops get transport wrapped"
    (let [seen-transport (atom nil)
          handler (fn [msg] (reset! seen-transport (:transport msg)))
          wrapped (nrepl/wrap-dt handler)
          session (atom {})
          orig-transport (reify t/Transport
                           (recv [_] nil)
                           (recv [_ _] nil)
                           (send [_ _] nil))]
      (wrapped {:op "eval" :session session :transport orig-transport})
      (is (some? @seen-transport))
      (is (not= orig-transport @seen-transport))))
  (testing "non-eval ops pass transport through"
    (let [seen-transport (atom nil)
          handler (fn [msg] (reset! seen-transport (:transport msg)))
          wrapped (nrepl/wrap-dt handler)
          session (atom {})
          orig-transport :my-transport]
      (wrapped {:op "describe" :session session :transport orig-transport})
      (is (= orig-transport @seen-transport)))))

(deftest wrap-dt-updates-dt-across-multiple-evals
  (testing "*dt* updates to latest dataset result"
    (let [ds1 (ds/->dataset {:x [1]})
          ds2 (ds/->dataset {:y [2]})
          {:keys [transport]} (make-recording-transport)
          session (atom {#'core/*dt* nil})
          wt (#'nrepl/wrapping-transport transport session)]
      (binding [core/*dt* nil]
        (binding [*1 ds1]
          (t/send wt {:value "ds1"})
          (is (= ds1 core/*dt*)))
        (binding [*1 ds2]
          (t/send wt {:value "ds2"})
          (is (= ds2 core/*dt*)))))))

(deftest middleware-descriptor
  (testing "set-descriptor! registered correctly"
    (let [desc-key (keyword "nrepl.middleware" "descriptor")
          desc (get (meta #'nrepl/wrap-dt) desc-key)]
      (is (some? desc))
      (is (contains? (:requires desc) "session"))
      (is (contains? (:expects desc) "eval")))))
