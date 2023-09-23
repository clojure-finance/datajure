(defproject datajure "0.1.0-SNAPSHOT"
  :description "An open-source domain-specific language for data processing."
  :url "https://clojure-finance.github.io/datajure-website/"
  :license {:name "The MIT License"
            :url "http://opensource.org/licenses/MIT"}
  :dependencies [[org.clojure/clojure "1.10.3"]
                 [org.apache.arrow/arrow-memory-unsafe "2.0.0"]
                 [org.apache.arrow/arrow-memory-core "2.0.0"]
                 [org.apache.arrow/arrow-vector "2.0.0" :exclusions [commons-codec]]
                 [techascent/tech.ml.dataset "6.104"]
                 [scicloj/tablecloth "6.103.1"]
                 [com.github.clojure-finance/clojask "2.0.0"]
                 [org.clojure/algo.generic "0.1.3"]
                 [zero.one/geni "0.0.40"]
                 [com.fasterxml.jackson.core/jackson-core "2.15.2"]
                 [metrics-clojure "2.10.0"]]
  :main ^:skip-aot datajure.dsl
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :jvm-opts ["-Dclojure.compiler.direct-linking=true"]}
             :provided {:dependencies [;; Spark
                                       [org.apache.spark/spark-avro_2.12 "3.1.1"]
                                       [org.apache.spark/spark-core_2.12 "3.1.1"]
                                       [org.apache.spark/spark-hive_2.12 "3.1.1"]
                                       [org.apache.spark/spark-mllib_2.12 "3.1.1"]
                                       [org.apache.spark/spark-sql_2.12 "3.1.1"]
                                       [org.apache.spark/spark-streaming_2.12 "3.1.1"]
                                       [com.github.fommil.netlib/all "1.1.2" :extension "pom"]
                                       ;; Arrow
                                       [org.apache.arrow/arrow-memory-netty "2.0.0"]
                                       [org.apache.arrow/arrow-memory-core "2.0.0"]
                                       [org.apache.arrow/arrow-vector "2.0.0"
                                        :exclusions [commons-codec com.fasterxml.jackson.core/jackson-databind]]
                                       ;; Databases
                                       [mysql/mysql-connector-java "8.0.23"]
                                       [org.postgresql/postgresql "42.2.19"]
                                       [org.xerial/sqlite-jdbc "3.34.0"]
                                       ;; Optional: Spark XGBoost
                                       [ml.dmlc/xgboost4j-spark_2.12 "1.2.0"]
                                       [ml.dmlc/xgboost4j_2.12 "1.2.0"]]}})
