#!/usr/bin/env bash
set -e
clojure -Sdeps '{:paths ["src" "resources" "test"] :deps {org.clojure/clojure {:mvn/version "1.12.4"} techascent/tech.ml.dataset {:mvn/version "8.003"}}}' \
  -M -e "
(require '[clojure.test :as t])
(load-file \"test/datajure/core_test.clj\")
(load-file \"test/datajure/concise_test.clj\")
(load-file \"test/datajure/util_test.clj\")
(load-file \"test/datajure/io_test.clj\")
(load-file \"test/datajure/reshape_test.clj\")
(load-file \"test/datajure/join_test.clj\")
(load-file \"test/datajure/asof_test.clj\")
(let [result (t/run-tests
               'datajure.core-test 'datajure.concise-test
               'datajure.util-test 'datajure.io-test
               'datajure.reshape-test 'datajure.join-test
               'datajure.asof-test)]
  (System/exit (+ (:fail result) (:error result))))"
