(defproject dp.dsl "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.apache.arrow/arrow-memory-unsafe "2.0.0"]
                 [org.apache.arrow/arrow-memory-core "2.0.0"]
                 [org.apache.arrow/arrow-vector "2.0.0" :exclusions [commons-codec]]
                 [techascent/tech.ml.dataset "6.042"]
                 [org.clojure/algo.generic "0.1.3"]]
  :main ^:skip-aot dp.dsl
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}})
