(ns datajure.repl
  (:require
   [clojure.string]
   [clojure.java.io :as io]
   [nrepl.server]
   [reply.main]))

(defn- client [opts]
  (let [port (:port opts)
        host (or (:host opts) "127.0.0.1")
        default-opts {:color true :history-file ".nrepl-history"}
        opts (assoc (merge default-opts opts) :attach (str host ":" port))]
    (reply.main/launch-nrepl opts)))

(defn datajure-prompt
  "Custom Datajure REPL prompt."
  [ns-]
  (str "datajure-repl (" ns- ")> "))

(defn launch-repl
  "Starts an nREPL server and steps into a REPL-y."
  [opts]
  (let [port   (:port opts)
        server (nrepl.server/start-server :port port)]
    (doto (io/file ".nrepl-port") .deleteOnExit (spit port))
    (client (merge {:custom-prompt datajure-prompt} opts))
    (nrepl.server/stop-server server)))

(def welcome-note
  "A REPL welcome note."
  (str "Datajure " "1.1.0"))