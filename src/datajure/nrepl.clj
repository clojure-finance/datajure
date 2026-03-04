(ns datajure.nrepl
  "nREPL middleware that automatically binds datajure.core/*dt* to the last
  dataset result in an interactive REPL session.

  Usage — add to your nREPL middleware stack in .nrepl.edn or deps.edn alias:

    ;; .nrepl.edn (recommended)
    {:middleware [datajure.nrepl/wrap-dt]}

    ;; deps.edn :nrepl alias
    {:main-opts [\"-m\" \"nrepl.cmdline\"
                 \"--middleware\" \"[datajure.nrepl/wrap-dt]\"]}

  After loading, any nREPL eval result that is a tech.v3.dataset is
  automatically bound to datajure.core/*dt* in the session, mirroring
  how Clojure's *1 works."
  (:require [nrepl.middleware :refer [set-descriptor!]]
            [nrepl.transport :as t]
            [datajure.core :as core])
  (:import [tech.v3.dataset.impl.dataset Dataset]))

(defn- dataset? [x]
  (instance? Dataset x))

(defn- wrapping-transport
  "Wraps a transport to intercept :value responses. When the eval result
  (*1, live on the session thread at send time) is a dataset, sets *dt*
  via set! so it is captured by the session's get-thread-bindings."
  [transport session]
  (reify t/Transport
    (recv [_] (t/recv transport))
    (recv [_ timeout] (t/recv transport timeout))
    (send [_ response]
      (when (contains? response :value)
        (let [v *1]
          (when (dataset? v)
            (when (thread-bound? #'core/*dt*)
              (set! core/*dt* v)))))
      (t/send transport response))))

(defn wrap-dt
  "nREPL middleware that binds datajure.core/*dt* to the last dataset result.

  Install via .nrepl.edn:
    {:middleware [datajure.nrepl/wrap-dt]}"
  [h]
  (fn [{:keys [op session] :as msg}]
    (when (and session (not (contains? @session #'core/*dt*)))
      (swap! session assoc #'core/*dt* nil))
    (h (if (= op "eval")
         (update msg :transport wrapping-transport session)
         msg))))

(set-descriptor! #'wrap-dt
                 {:requires #{"clone" "session"}
                  :expects #{"eval"}
                  :handles {}})
