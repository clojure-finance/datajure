(ns datajure.expr
  "AST definition and compiler for #dt/e expressions.

  #dt/e is a reader tag that produces an AST map. datajure.core interprets
  these ASTs when executing dt queries. This namespace handles:
    - AST node constructors
    - compile-expr: AST -> fn of dataset -> column/scalar
    - Reader tag handler (registered on require via alter-var-root)

  Auto-registration: loading this namespace (via datajure.core) merges the
  #dt/e data reader into *data-readers* at load time -- same pattern as
  clojure.instant / #inst. This is an intentional side effect; document in
  consumer namespaces. Use (datajure.expr/register-reader!) for AOT/script
  edge cases.

  Nil-safety rules (matching spec):
    - Comparison ops with nil arg -> false column (all rows false)
    - Arithmetic ops with nil arg -> nil (becomes missing when stored in dataset)
  These rules only activate when a Clojure nil literal appears in an expression.
  Dataset columns with missing values are handled natively by dfn."
  (:require [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset :as ds]))

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
;; Op dispatch table
;; ---------------------------------------------------------------------------

(def ^:private comparison-ops #{'> '< '>= '<= '= 'and 'or 'not})

(def ^:private op-table
  {'+ dfn/+
   '- dfn/-
   '* dfn/*
   '/ dfn//
   'sq dfn/sq
   'log dfn/log
   '> dfn/>
   '< dfn/<
   '>= dfn/>=
   '<= dfn/<=
   '= dfn/eq
   'and dfn/and
   'or dfn/or
   'not dfn/not})

;; ---------------------------------------------------------------------------
;; AST builder: Clojure form -> AST
;; ---------------------------------------------------------------------------

(defn- parse-form [form]
  (cond
    (keyword? form) (col-node form)
    (symbol? form) (lit-node form)
    (seq? form) (let [[op & args] form]
                  (op-node op (mapv parse-form args)))
    :else (lit-node form)))

;; ---------------------------------------------------------------------------
;; Compiler: AST -> fn of dataset
;; ---------------------------------------------------------------------------

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
    :op (let [op-sym (:op/name node)
              op-fn (or (op-table op-sym)
                        (throw (ex-info "Unknown op in #dt/e expression"
                                        {:op op-sym})))
              arg-fns (mapv compile-expr (:op/args node))
              cmp? (comparison-ops op-sym)]
          (fn [ds]
            (let [args (map #(% ds) arg-fns)]
              (if (some nil? args)
                (if cmp?
                  (boolean-array (ds/row-count ds) false)
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
  "Register the #dt/e reader tag in *data-readers*. Called automatically
  when this namespace is loaded."
  []
  (alter-var-root #'*data-readers* assoc 'dt/e #'read-expr))

(register-reader!)
