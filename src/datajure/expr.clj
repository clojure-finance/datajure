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
            [tech.v3.dataset :as ds]))

;; ---------------------------------------------------------------------------
;; Op dispatch table: symbol -> keyword -> dfn fn
;; ---------------------------------------------------------------------------

(def ^:private comparison-ops #{:> :< :>= :<= := :and :or :not :in :between?})

(def ^:private op-table
  {:+ dfn/+
   :- dfn/-
   :* dfn/*
   :div (fn [a b] (dfn// (dfn/double a) (dfn/double b)))
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
   :between? (fn [col lo hi] (dfn/and (dfn/>= col lo) (dfn/<= col hi)))})

(def ^:private sym->op
  "Maps source-form symbols to canonical keyword op names."
  {'+ :+, '- :-, '* :*, '/ :div
   'sq :sq, 'log :log
   '> :>, '< :<, '>= :>=, '<= :<=, '= :=
   'and :and, 'or :or, 'not :not
   'mn :mn, 'sm :sm, 'md :md, 'sd :sd
   'in :in, 'between? :between?})

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

(defn col-node [kw]
  {:node/type :col :col/name kw})

(defn lit-node [v]
  {:node/type :lit :lit/value v})

(defn op-node [op args]
  {:node/type :op :op/name op :op/args args})

;; ---------------------------------------------------------------------------
;; AST builder: Clojure form -> AST
;; ---------------------------------------------------------------------------

(defn- parse-form [form]
  (cond
    (keyword? form) (col-node form)
    (seq? form) (let [[op & args] form]
                  (op-node (->op-kw op) (mapv parse-form args)))
    :else (lit-node form)))

;; ---------------------------------------------------------------------------
;; Compiler: AST -> fn of dataset
;; ---------------------------------------------------------------------------

(defn col-refs
  "Extract the set of column keywords referenced by an AST node."
  [node]
  (case (:node/type node)
    :col #{(:col/name node)}
    :lit #{}
    :op (into #{} (mapcat col-refs) (:op/args node))))

(defn compile-expr
  "Compile an AST node to a fn [ds] -> column/scalar.
  Column keywords resolve to dataset columns; literals pass through;
  ops dispatch via op-table to dfn functions.

  Nil-safety: if any arg to an op evaluates to nil (e.g. a nil literal),
  comparison ops return an all-false boolean column; arithmetic ops return nil
  (which becomes a missing value when stored in a dataset column)."
  [node]
  (case (:node/type node)
    :col (fn [ds] (ds (:col/name node)))
    :lit (fn [_ds] (:lit/value node))
    :op (let [op-kw (:op/name node)
              op-fn (or (op-table op-kw)
                        (throw (ex-info "Unknown op in #dt/e expression"
                                        {:op op-kw})))
              arg-fns (mapv compile-expr (:op/args node))
              cmp? (comparison-ops op-kw)]
          (fn [ds]
            (let [args (map #(% ds) arg-fns)]
              (if (some nil? args)
                (if cmp?
                  (dtype/make-reader :boolean (ds/row-count ds) false)
                  nil)
                (apply op-fn args)))))))

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
