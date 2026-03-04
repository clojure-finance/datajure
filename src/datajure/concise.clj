(ns datajure.concise
  "Opt-in short aliases for power users.
  Refer the symbols you want: (require '[datajure.concise :refer [mn sm mx mi N dt fst lst wa ws]])

  Aggregation helpers (operate on column vectors — use in :agg plain fns or directly):
    mn    = mean
    sm    = sum
    md    = median
    sd    = standard-deviation
    ct    = count (element count)
    nuniq = count-distinct
    fst   = first-val (first element)
    lst   = last-val (last element)
    wa    = wavg (weighted average)
    ws    = wsum (weighted sum)
    mx    = max* (column maximum)
    mi    = min* (column minimum)

  Everything else re-exported from datajure.core:
    N, dt, asc, desc, rename, pass-nil"
  (:require [tech.v3.datatype :as dtype]
            [datajure.core :as core]
            [datajure.expr :as expr]))

(def ^{:doc "Column mean. Short alias for `core/mean`."} mn core/mean)
(def ^{:doc "Column sum. Short alias for `core/sum`."} sm core/sum)
(def ^{:doc "Column median. Short alias for `core/median`."} md core/median)
(def ^{:doc "Column standard deviation. Short alias for `core/stddev`."} sd core/stddev)
(def ^{:doc "Element count. Short alias for `dtype/ecount`."} ct dtype/ecount)

(def ^{:doc "Count of distinct values. Delegates to `expr/count-distinct`."} nuniq expr/count-distinct)

(def ^{:doc "Row count helper. Re-exported from `datajure.core/N`."} N core/N)
(def ^{:doc "Main query function. Re-exported from `datajure.core/dt`."} dt core/dt)
(def ^{:doc "Ascending sort spec. Re-exported from `datajure.core/asc`."} asc core/asc)
(def ^{:doc "Descending sort spec. Re-exported from `datajure.core/desc`."} desc core/desc)
(def ^{:doc "Rename columns. Re-exported from `datajure.core/rename`."} rename core/rename)
(def ^{:doc "Nil-safe function wrapper. Re-exported from `datajure.core/pass-nil`."} pass-nil core/pass-nil)

(def ^{:doc "First value in a column. Delegates to `expr/first-val`."} fst expr/first-val)
(def ^{:doc "Last value in a column. Delegates to `expr/last-val`."} lst expr/last-val)
(def ^{:doc "Weighted average. Delegates to `expr/wavg`. Args: weight-col value-col."} wa expr/wavg)
(def ^{:doc "Weighted sum. Delegates to `expr/wsum`. Args: weight-col value-col."} ws expr/wsum)

(def ^{:doc "Column maximum. Short alias for `core/max*`."} mx core/max*)
(def ^{:doc "Column minimum. Short alias for `core/min*`."} mi core/min*)
