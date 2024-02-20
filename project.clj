(defproject com.github.clojure-finance/datajure "1.1.0"
  :description "An open-source domain-specific language for data processing."
  :url "https://clojure-finance.github.io/datajure-website/"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[log4j/log4j "1.2.17"]
                 [org.apache.logging.log4j/log4j-core "2.21.0"]
                 [com.fasterxml.jackson.core/jackson-core "2.15.3"]
                 [com.fasterxml.jackson.core/jackson-annotations "2.15.3"]
                 [org.clojure/clojure "1.11.1"]
                 [techascent/tech.ml.dataset "7.020"]
                 [scicloj/tablecloth "7.014"]
                 [com.github.clojure-finance/clojask "2.0.2"]
                 [zero.one/geni "0.0.42"]
                 [com.fasterxml.jackson.core/jackson-core "2.15.3"]
                 [metrics-clojure "2.10.0"]
                 [org.apache.poi/poi "5.2.4"]
                 [org.apache.zookeeper/zookeeper "3.7.2" :exclusions [org.slf4j/slf4j-log4j12]]
                 [org.apache.arrow/arrow-memory-core "4.0.0"]
                 [org.apache.arrow/arrow-vector "4.0.0"
                  :exclusions [commons-codec com.fasterxml.jackson.core/jackson-databind]]
                 [org.apache.spark/spark-avro_2.12 "3.3.3"]
                 [org.apache.spark/spark-core_2.12 "3.3.3" :exclusions [org.apache.logging.log4j/log4j-slf4j-impl]]
                 [org.apache.spark/spark-hive_2.12 "3.3.3"]
                 [org.apache.spark/spark-mllib_2.12 "3.3.3"]
                 [org.apache.spark/spark-sql_2.12 "3.3.3"]
                 [org.apache.spark/spark-streaming_2.12 "3.3.3"]]
  :main ^:skip-aot datajure.dsl
  :plugins [[dev.weavejester/lein-cljfmt "0.12.0"]]
  :target-path "target/%s"
  :jvm-opts ["--add-opens=java.base/java.nio=ALL-UNNAMED"
             "--add-opens=java.base/java.net=ALL-UNNAMED"
             "--add-opens=java.base/java.lang=ALL-UNNAMED"
             "--add-opens=java.base/java.util=ALL-UNNAMED"
             "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED"
             "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"]
  :profiles {:uberjar {:aot :all
                       :plugins [[arctype/log4j2-plugins-cache "1.0.0"]]
                       :middleware [leiningen.log4j2-plugins-cache/middleware]
                       :manifest {"Multi-Release" true}}
             :test {:dependencies [[org.apache.logging.log4j/log4j-core "2.21.0"]]}
             :repl {:dependencies [[org.apache.logging.log4j/log4j-core "2.21.0"]]}})