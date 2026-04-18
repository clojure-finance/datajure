(ns build
  (:require [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'com.github.clojure-finance/datajure)
(def version "2.0.8")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :scm {:url "https://github.com/clojure-finance/datajure"
                      :connection "scm:git:git://github.com/clojure-finance/datajure.git"
                      :developerConnection "scm:git:ssh://git@github.com/clojure-finance/datajure.git"
                      :tag (str "v" version)}
                :basis (b/create-basis {:project "deps.edn"})
                :src-dirs ["src" "resources"]
                :pom-data [[:licenses
                            [:license
                             [:name "Eclipse Public License 2.0"]
                             [:url "https://www.eclipse.org/legal/epl-2.0/"]]]
                           [:description
                            "Clojure data manipulation DSL built on tech.ml.dataset"]
                           [:url
                            "https://github.com/clojure-finance/datajure"]]})
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn deploy [_]
  (dd/deploy {:installer :remote
              :artifact jar-file
              :pom-file (b/pom-path {:lib lib
                                     :class-dir class-dir})}))
