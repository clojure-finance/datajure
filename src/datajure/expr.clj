(ns datajure.expr
  "AST definition and compiler for #dt/e expressions.

  #dt/e is a reader tag that produces an AST map. datajure.core interprets
  these ASTs when executing dt queries. This namespace handles:
    - AST node constructors
    - compile-expr: AST -> fn of dataset -> column/scalar
    - Reader tag handler registered via resources/data_readers.clj (primary)
      and register-reader! / alter-var-root (AOT/script fallback)

  Op names are stored as keywords in the AST (e.g. :and, :>, :+) rather than
  symbols, because the Clojure compiler tries to resolve symbols in literal
  data structures. Since and/or/not are macros, the compiler rejects
  'Can't take value of a macro' when it encounters them as bare symbols in
  the map values returned by the reader tag. Keywords are self-evaluating
  and avoid this entirely.

  Nil-safety rules (matching spec):
    - Comparison ops with nil arg -> false column (all rows false)
    - Arithmetic ops with nil arg -> nil (becomes missing when stored in dataset)
  These rules only activate when a Clojure nil literal appears in an expression.
  Dataset columns with missing values are handled natively by dfn."
  (:require [tech.v3.datatype :as dtype]
            [tech.v3.datatype.functional :as dfn]
            [tech.v3.datatype.datetime :as dtype-dt]
            [tech.v3.dataset :as ds]
            [datajure.window :as win]
            [datajure.row :as row]))

;; ---------------------------------------------------------------------------
;; Op dispatch table: symbol -> keyword -> dfn fn
;; ---------------------------------------------------------------------------

(def ^:private comparison-ops #{:> :< :>= :<= := :and :or :not :in :between?})

(def ^:private win-ops
  "Valid window operation keywords."
  #{:win/rank :win/dense-rank :win/row-number
    :win/lag :win/lead
    :win/cumsum :win/cummin :win/cummax :win/cummean
    :win/rleid
    :win/delta :win/ratio :win/differ
    :win/mavg :win/msum :win/mdev :win/mmin :win/mmax
    :win/ema :win/fills})

(def ^:private win-sym->op
  "Maps win/* source symbols to canonical keyword op names."
  {'win/rank :win/rank
   'win/dense-rank :win/dense-rank
   'win/row-number :win/row-number
   'win/lag :win/lag
   'win/lead :win/lead
   'win/cumsum :win/cumsum
   'win/cummin :win/cummin
   'win/cummax :win/cummax
   'win/cummean :win/cummean
   'win/rleid :win/rleid
   'win/delta :win/delta
   'win/ratio :win/ratio
   'win/differ :win/differ
   'win/mavg :win/mavg
   'win/msum :win/msum
   'win/mdev :win/mdev
   'win/mmin :win/mmin
   'win/mmax :win/mmax
   'win/ema :win/ema
   'win/fills :win/fills})

(def ^:private win-op-table
  "Maps window op keywords to runtime functions from datajure.window."
  {:win/rank win/win-rank
   :win/dense-rank win/win-dense-rank
   :win/row-number win/win-row-number
   :win/lag win/win-lag
   :win/lead win/win-lead
   :win/cumsum win/win-cumsum
   :win/cummin win/win-cummin
   :win/cummax win/win-cummax
   :win/cummean win/win-cummean
   :win/rleid win/win-rleid
   :win/delta win/win-delta
   :win/ratio win/win-ratio
   :win/differ win/win-differ
   :win/mavg win/win-mavg
   :win/msum win/win-msum
   :win/mdev win/win-mdev
   :win/mmin win/win-mmin
   :win/mmax win/win-mmax
   :win/ema win/win-ema
   :win/fills win/win-fills})

(def ^:private row-sym->op
  "Maps row/* source symbols to canonical keyword op names."
  {'row/sum :row/sum
   'row/mean :row/mean
   'row/min :row/min
   'row/max :row/max
   'row/count-nil :row/count-nil
   'row/any-nil? :row/any-nil?})

(def ^:private row-op-table
  "Maps row op keywords to runtime functions from datajure.row."
  {:row/sum row/row-sum
   :row/mean row/row-mean
   :row/min row/row-min
   :row/max row/row-max
   :row/count-nil row/row-count-nil
   :row/any-nil? row/row-any-nil?})

(defn count-distinct
  "Count of distinct non-nil values in a column."
  [col]
  (count (distinct (dtype/->reader col))))

(defn first-val
  "First value in a column."
  [col]
  (first (dtype/->reader col)))

(defn last-val
  "Last value in a column."
  [col]
  (let [r (dtype/->reader col)]
    (nth r (dec (dtype/ecount r)))))

(defn wavg
  "Weighted average. Args: weight-col value-col. Skips nil pairs."
  [w v]
  (let [wr (dtype/->reader w)
        vr (dtype/->reader v)
        n (dtype/ecount wr)
        pairs (keep (fn [i]
                      (let [wi (nth wr i) vi (nth vr i)]
                        (when (and (some? wi) (some? vi)) [wi vi])))
                    (range n))]
    (if (empty? pairs)
      nil
      (let [wvs (map (fn [[wi vi]] (* wi vi)) pairs)
            ws (map first pairs)]
        (/ (reduce + wvs) (reduce + ws))))))

(defn wsum
  "Weighted sum. Args: weight-col value-col. Skips nil pairs."
  [w v]
  (let [wr (dtype/->reader w)
        vr (dtype/->reader v)
        n (dtype/ecount wr)
        wvs (keep (fn [i]
                    (let [wi (nth wr i) vi (nth vr i)]
                      (when (and (some? wi) (some? vi)) (* wi vi))))
                  (range n))]
    (if (empty? wvs) nil (reduce + wvs))))

(def ^:private op-table
  {:+ dfn/+
   :- dfn/-
   :* dfn/*
   :div (fn [a b] (dfn// (dfn/double a) (dfn/double b)))
   :div0 (fn [a b]
           (let [av (dfn/double a)
                 bv (dfn/double b)]
             (dtype/make-reader :float64 (dtype/ecount bv)
                                (let [bi (nth bv idx)]
                                  (if (or (nil? bi) (zero? bi))
                                    nil
                                    (let [ai (nth av idx)]
                                      (if (nil? ai) nil (/ ai bi))))))))
   :sq dfn/sq
   :log dfn/log
   :> dfn/>
   :< dfn/<
   :>= dfn/>=
   :<= dfn/<=
   := dfn/eq
   :and dfn/and
   :or dfn/or
   :not dfn/not
   :mn dfn/mean
   :sm dfn/sum
   :md dfn/median
   :sd dfn/standard-deviation
   :in (fn [col s]
         (dtype/make-reader :boolean (dtype/ecount col)
                            (boolean (contains? s (nth col idx)))))
   :between? (fn [col lo hi] (dfn/and (dfn/>= col lo) (dfn/<= col hi)))
   :nuniq count-distinct
   :first-val first-val
   :last-val last-val
   :wavg wavg
   :wsum wsum})

(def ^:private sym->op
  "Maps source-form symbols to canonical keyword op names."
  {'+ :+, '- :-, '* :*, '/ :div
   'sq :sq, 'log :log
   '> :>, '< :<, '>= :>=, '<= :<=, '= :=
   'and :and, 'or :or, 'not :not
   'mn :mn, 'sm :sm, 'md :md, 'sd :sd
   'in :in, 'between? :between?, 'count-distinct :nuniq
   'first-val :first-val, 'last-val :last-val
   'wavg :wavg, 'wsum :wsum
   'div0 :div0})

(defn- ->op-kw
  "Normalise a source-form op to its canonical keyword.
  At read time, ops arrive as plain symbols ('and, '>, etc.)."
  [op]
  (or (sym->op op)
      (when (keyword? op) op)
      (throw (ex-info "Unknown op in #dt/e expression"
                      {:op op :op-type (type op)}))))

;; ---------------------------------------------------------------------------
;; AST node constructors
;; ---------------------------------------------------------------------------

(defn col-node
  "Create a column reference AST node. `kw` is a keyword naming the column."
  [kw]
  {:node/type :col :col/name kw})

(defn expr-node?
  "Returns true if x is a #dt/e AST node (a map with :node/type)."
  [x]
  (and (map? x) (contains? x :node/type)))

(defn lit-node
  "Create a literal value AST node. `v` is any scalar (number, string, keyword, set, etc.)."
  [v]
  {:node/type :lit :lit/value v})

(defn op-node
  "Create an operation AST node. `op` is an op keyword (e.g. :+, :>), `args` is a seq of child AST nodes."
  [op args]
  {:node/type :op :op/name op :op/args args})

(defn win-node
  "AST node for a window function call.
  win-op is a keyword like :win/rank. args are parsed AST nodes."
  [win-op args]
  {:node/type :win :win/op win-op :win/args args})

(defn row-node
  "AST node for a row-wise function call.
  row-op is a keyword like :row/sum. args are parsed AST nodes."
  [row-op args]
  {:node/type :row :row/op row-op :row/args args})

;; ---------------------------------------------------------------------------
;; AST builder: Clojure form -> AST
;; ---------------------------------------------------------------------------

(defn- parse-form
  ([form] (parse-form form #{}))
  ([form env]
   (cond
     (keyword? form) (col-node form)
     (and (symbol? form) (contains? env form)) {:node/type :binding-ref :binding-ref/name (keyword form)}
     (seq? form)
     (let [[op & args] form]
       (cond
         (= op 'if)
         (let [[pred then else] args]
           {:node/type :if
            :if/pred (parse-form pred env)
            :if/then (parse-form then env)
            :if/else (if (some? else) (parse-form else env) (lit-node nil))})
         (= op 'cond)
         (let [pairs (partition 2 args)]
           (reduce (fn [else-node [test then]]
                     {:node/type :if
                      :if/pred (if (= test :else) (lit-node true) (parse-form test env))
                      :if/then (parse-form then env)
                      :if/else else-node})
                   (lit-node nil)
                   (reverse pairs)))
         (= op 'coalesce)
         {:node/type :coalesce :coalesce/args (mapv #(parse-form % env) args)}
         (= op 'let)
         (let [[binding-vec body] args
               binding-pairs (partition 2 binding-vec)
               [bindings new-env]
               (reduce (fn [[acc env] [sym expr]]
                         [(conj acc {:binding/name (keyword sym)
                                     :binding/expr (parse-form expr env)})
                          (conj env sym)])
                       [[] env]
                       binding-pairs)]
           {:node/type :let
            :let/bindings bindings
            :let/body (parse-form body new-env)})
         (= op 'cut)
         (let [[col-form n-form & rest-args] args
               from-expr (when (= (first rest-args) :from) (second rest-args))]
           {:node/type :cut
            :cut/col (parse-form col-form env)
            :cut/n (parse-form n-form env)
            :cut/from (when from-expr (parse-form from-expr env))})
         (= op 'xbar)
         (let [[col-form width-form unit-kw] args]
           {:node/type :xbar
            :xbar/col (parse-form col-form env)
            :xbar/width (parse-form width-form env)
            :xbar/unit unit-kw})
         (= op 'win/scan)
         (let [[scan-sym col-form] args
               scan-op-kw (keyword (name scan-sym))]
           {:node/type :scan
            :scan/op scan-op-kw
            :scan/arg (parse-form col-form env)})
         (contains? win-sym->op op)
         (win-node (win-sym->op op) (mapv #(parse-form % env) args))
         (contains? row-sym->op op)
         (row-node (row-sym->op op) (mapv #(parse-form % env) args))
         :else
         (op-node (->op-kw op) (mapv #(parse-form % env) args))))
     :else (lit-node form))))

;; ---------------------------------------------------------------------------
;; Compiler: AST -> fn of dataset
;; ---------------------------------------------------------------------------

(defn col-refs
  "Extract the set of column keywords referenced by an AST node.
  If a :lit node contains an embedded expr-node (from expression composition),
  recursively extracts its col-refs."
  [node]
  (case (:node/type node)
    :col #{(:col/name node)}
    :lit (let [v (:lit/value node)]
           (if (expr-node? v) (col-refs v) #{}))
    :binding-ref #{}
    :op (into #{} (mapcat col-refs) (:op/args node))
    :win (into #{} (mapcat col-refs) (:win/args node))
    :scan (col-refs (:scan/arg node))
    :cut (let [base (into (col-refs (:cut/col node)) (col-refs (:cut/n node)))]
           (if-let [from (:cut/from node)]
             (into base (col-refs from))
             base))
    :xbar (into (col-refs (:xbar/col node)) (col-refs (:xbar/width node)))
    :row (into #{} (mapcat col-refs) (:row/args node))
    :if (into (col-refs (:if/pred node))
              (concat (col-refs (:if/then node))
                      (col-refs (:if/else node))))
    :coalesce (into #{} (mapcat col-refs) (:coalesce/args node))
    :let (into (into #{} (mapcat (comp col-refs :binding/expr)) (:let/bindings node))
               (col-refs (:let/body node)))))

(defn win-refs
  "Extract the set of window op keywords (e.g. :win/rank) referenced by an AST node.
  If a :lit node contains an embedded expr-node (from expression composition),
  recursively extracts its win-refs."
  [node]
  (case (:node/type node)
    :col #{}
    :lit (let [v (:lit/value node)]
           (if (expr-node? v) (win-refs v) #{}))
    :binding-ref #{}
    :op (into #{} (mapcat win-refs) (:op/args node))
    :win (into #{(:win/op node)} (mapcat win-refs) (:win/args node))
    :scan (conj (win-refs (:scan/arg node)) :win/scan)
    :cut (let [base (into (win-refs (:cut/col node)) (win-refs (:cut/n node)))]
           (if-let [from (:cut/from node)]
             (into base (win-refs from))
             base))
    :xbar (into (win-refs (:xbar/col node)) (win-refs (:xbar/width node)))
    :row (into #{} (mapcat win-refs) (:row/args node))
    :if (into (win-refs (:if/pred node))
              (concat (win-refs (:if/then node))
                      (win-refs (:if/else node))))
    :coalesce (into #{} (mapcat win-refs) (:coalesce/args node))
    :let (into (into #{} (mapcat (comp win-refs :binding/expr)) (:let/bindings node))
               (win-refs (:let/body node)))))

(defn- xbar-bucket
  "Floor-divide a column by width to produce xbar bucket values.
  For numeric columns: quot(col, width) * width.
  For datetime columns: convert to epoch-milliseconds, divide by ms-per-unit,
  then floor-divide by width. Returns a reader of longs (nil preserved for missing)."
  [col width unit]
  (let [col-dtype (-> col meta :datatype)
        n (dtype/ecount col)
        missing (tech.v3.dataset.column/missing col)]
    (if (dtype-dt/datetime-datatype? col-dtype)
      (let [ms-per-unit (condp = unit
                          :seconds dtype-dt/milliseconds-in-second
                          :minutes dtype-dt/milliseconds-in-minute
                          :hours dtype-dt/milliseconds-in-hour
                          :days dtype-dt/milliseconds-in-day
                          :weeks dtype-dt/milliseconds-in-week
                          (throw (ex-info (str "Unknown xbar temporal unit: " unit)
                                          {:dt/error :xbar-unknown-unit :unit unit})))]
        (dtype/make-reader :object n
                           (if (.contains missing idx)
                             nil
                             (let [epoch-ms (dtype-dt/datetime->epoch :epoch-milliseconds (nth col idx))
                                   epoch-units (quot epoch-ms ms-per-unit)]
                               (* width (quot epoch-units width))))))
      (dtype/make-reader :object n
                         (if (.contains missing idx)
                           nil
                           (let [v (long (nth col idx))]
                             (* width (quot v width))))))))

(defn- cut-bucket
  "Assign each element of col to an equal-count bin in 1..n.
  Breakpoints are computed from the reference population:
  - mask nil: all non-nil values of col (plain cut).
  - mask a boolean reader: col values where mask is true and col is non-nil.
    This is the NYSE-style use case: mask selects a reference subpopulation
    (e.g. NYSE stocks only); breakpoints are computed from that subset and
    applied to all rows of col.
  Bin assignment uses right-open intervals (Java binarySearch). nil values in
  col produce nil. All non-nil values land in [1, n]."
  ([col n] (cut-bucket col n nil))
  ([col n mask]
   (let [n (int n)
         cnt (dtype/ecount col)
         missing (tech.v3.dataset.column/missing col)
         col-reader (dtype/->reader col)
         ref-pop (if (some? mask)
                   (filterv some?
                            (map-indexed (fn [i v] (when (nth mask i) v)) col-reader))
                   (filterv some? col-reader))
         pcts (mapv #(* % (/ 100.0 n)) (range 1 n))
         breaks-arr (if (empty? pcts)
                      (double-array 0)
                      (double-array (dfn/percentiles ref-pop pcts {:nan-strategy :remove})))]
     (dtype/make-reader :object cnt
                        (if (.contains missing idx)
                          nil
                          (let [v (double (nth col-reader idx))
                                r (java.util.Arrays/binarySearch breaks-arr v)
                                b (if (>= r 0) r (- (- r) 1))]
                            (min (inc b) n)))))))

(defn compile-expr
  "Compile an AST node to a fn [ds] -> column/scalar.
  Column keywords resolve to dataset columns; literals pass through;
  ops dispatch via op-table to dfn functions.

  Nil-safety: if any arg to an op evaluates to nil (e.g. a nil literal),
  comparison ops return an all-false boolean column; arithmetic ops return nil
  (which becomes a missing value when stored in a dataset column)."
  ([node] (compile-expr node {}))
  ([node env]
   (case (:node/type node)
     :col (fn [ds] (ds (:col/name node)))
     :lit (let [v (:lit/value node)]
            (if (expr-node? v)
              (compile-expr v env)
              (fn [_ds] v)))
     :binding-ref (let [kw (:binding-ref/name node)
                        bound-fn (get env kw)]
                    (fn [ds] (bound-fn ds)))
     :if (let [pred-fn (compile-expr (:if/pred node) env)
               then-fn (compile-expr (:if/then node) env)
               else-fn (compile-expr (:if/else node) env)]
           (fn [ds]
             (let [pred (pred-fn ds)
                   then (then-fn ds)
                   else (else-fn ds)
                   n (ds/row-count ds)]
               (cond
                 (nil? pred) else
                 (true? pred) then
                 (false? pred) else
                 :else
                 (dtype/make-reader :object n
                                    (let [t (if (dtype/reader? then) (nth then idx) then)
                                          e (if (dtype/reader? else) (nth else idx) else)]
                                      (if (nth pred idx) t e)))))))
     :coalesce (let [arg-fns (mapv #(compile-expr % env) (:coalesce/args node))]
                 (fn [ds]
                   (let [n (ds/row-count ds)
                         cols (mapv #(% ds) arg-fns)]
                     (dtype/make-reader :object n
                                        (let [vals (map #(if (dtype/reader? %) (nth % idx) %) cols)]
                                          (first (filter some? vals)))))))
     :let (let [final-env
                (reduce (fn [acc-env b]
                          (let [kw (:binding/name b)
                                compiled-fn (compile-expr (:binding/expr b) acc-env)]
                            (assoc acc-env kw compiled-fn)))
                        env
                        (:let/bindings node))
                body-fn (compile-expr (:let/body node) final-env)]
            (fn [ds] (body-fn ds)))
     :scan (let [op-kw (:scan/op node)
                 arg-fn (compile-expr (:scan/arg node) env)]
             (fn [ds]
               (win/win-scan op-kw (arg-fn ds))))
     :cut (let [col-fn (compile-expr (:cut/col node) env)
                n-fn (compile-expr (:cut/n node) env)
                mask-fn (when-let [from (:cut/from node)]
                          (compile-expr from env))]
            (fn [ds]
              (cut-bucket (col-fn ds) (n-fn ds) (when mask-fn (mask-fn ds)))))
     :xbar (let [col-fn (compile-expr (:xbar/col node) env)
                 width-fn (compile-expr (:xbar/width node) env)
                 unit (:xbar/unit node)]
             (fn [ds]
               (xbar-bucket (col-fn ds) (width-fn ds) unit)))
     :win (let [win-op-kw (:win/op node)
                win-fn (or (win-op-table win-op-kw)
                           (throw (ex-info (str "Unknown window op: " win-op-kw)
                                           {:dt/error :unknown-win-op :win-op win-op-kw})))
                arg-fns (mapv #(compile-expr % env) (:win/args node))]
            (fn [ds]
              (let [args (map #(% ds) arg-fns)]
                (apply win-fn args))))
     :row (let [row-op-kw (:row/op node)
                row-fn (or (row-op-table row-op-kw)
                           (throw (ex-info (str "Unknown row op: " row-op-kw)
                                           {:dt/error :unknown-row-op :row-op row-op-kw})))
                arg-fns (mapv #(compile-expr % env) (:row/args node))]
            (fn [ds]
              (let [args (map #(% ds) arg-fns)]
                (apply row-fn args))))
     :op (let [op-kw (:op/name node)
               op-fn (or (op-table op-kw)
                         (throw (ex-info "Unknown op in #dt/e expression"
                                         {:op op-kw})))
               arg-fns (mapv #(compile-expr % env) (:op/args node))
               cmp? (comparison-ops op-kw)]
           (fn [ds]
             (let [args (map #(% ds) arg-fns)]
               (if (some nil? args)
                 (if cmp?
                   (dtype/make-reader :boolean (ds/row-count ds) false)
                   nil)
                 (apply op-fn args))))))))

;; ---------------------------------------------------------------------------
;; Reader tag handler
;; ---------------------------------------------------------------------------

(defn read-expr
  "Reader tag handler for #dt/e. Returns an AST map. Called at read time."
  [form]
  (parse-form form))

(defn register-reader!
  "Register the #dt/e reader tag via alter-var-root on *data-readers*.
  Fallback for AOT compilation or scripts where data_readers.clj is not picked
  up at startup. The primary registration mechanism is resources/data_readers.clj,
  which Clojure merges automatically for all threads at JVM startup."
  []
  (alter-var-root #'*data-readers* assoc 'dt/e #'read-expr))
