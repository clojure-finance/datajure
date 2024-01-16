(defproject {{raw-name}} "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"
             "--add-opens=java.base/java.net=ALL-UNNAMED"
             "--add-opens=java.base/java.lang=ALL-UNNAMED"
             "--add-opens=java.base/java.util=ALL-UNNAMED"
             "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]
  :dependencies [[com.github.clojure-finance/datajure "1.1.0"]
                 [org.apache.logging.log4j/log4j-core "2.21.0"]
                 [org.clojure/clojure "1.11.1"]]
  :jar-name "{{raw-name}}.jar"
  :uberjar-name "{{raw-name}}-standalone.jar"
  :profiles {:uberjar {:aot :all}}
  :main ^:skip-aot {{namespace}}.core
  :target-path "target/%s")
