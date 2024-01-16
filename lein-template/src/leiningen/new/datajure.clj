(ns leiningen.new.datajure
  (:require [leiningen.new.templates :as tmpl]
            [leiningen.core.main :as main]))

(def render (tmpl/renderer "datajure"))

(defn datajure
  "A Datajure project template."
  [name]
  (let [main-ns (tmpl/sanitize-ns name)
        data    {:raw-name    name
                 :name        (tmpl/project-name name)
                 :namespace   main-ns
                 :nested-dirs (tmpl/name-to-path main-ns)}]
    (main/info "Generating fresh 'lein new' com.github.clojure-finance/datajure project.")
    (tmpl/->files data
                  [".gitignore" (render "gitignore" data)]
                  ["README.md" (render "README.md" data)]
                  ["project.clj" (render "project.clj" data)]
                  ["src/{{nested-dirs}}/core.clj" (render "core.clj" data)])))
