(ns datajure.expr
  "AST definition and compiler for #dt/e expressions.

  #dt/e is a reader tag that produces an AST map. datajure.core interprets
  these ASTs when executing dt queries. This namespace handles:
    - AST node constructors
    - compile-expr: AST -> fn of dataset -> column/scalar
    - Reader tag handler registered via resources/data_readers.clj (primary)
      and register-reader! / alter-var-root (AOT/script fallback)

  For #dt/e to work at the REPL, users must require this namespace (or
  datajure.core). This resolves the exported vars sq, log, and, or, not
  which shadow clojure.core macros/fns so the reader sees them as plain vars.

  Nil-safety rules (matching spec):
    - Comparison ops with nil arg -> false column (all rows false)
    - Arithmetic ops with nil arg -> nil (becomes missing when stored in dataset)
  These rules only activate when a Clojure nil literal appears in an expression.
  Dataset columns with missing values are handled natively by dfn."
  (:require [tech.v3.datatype.functional :as dfn]
            [tech.v3.dataset :as ds]))

;; ---------------------------------------------------------------------------
;; Exported stub vars for ops that don't exist in clojure.core
;; (sq, log) or that are macros in clojure.core (and, or, not).
;; These stubs are never called directly -- the AST compiler always uses
;; op-table. They exist solely so the reader can resolve them as plain vars.
;; ---------------------------------------------------------------------------

(def sq "Marker var for #dt/e sq — resolved by the expression compiler." ::sq)
(def log "Marker var for #dt/e log — resolved by the expression compiler." ::log)
(def and "Marker var for #dt/e and — shadows clojure.core/and macro." ::and)
(def or "Marker var for #dt/e or — shadows clojure.core/or macro." ::or)
(def not "Marker var for #dt/e not — resolved by the expression compiler." ::not)

;; ---------------------------------------------------------------------------
;; Op dispatch table: symbol -> dfn fn
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

;; Reverse map: resolved fn/var value -> canonical symbol.
;; Needed because nREPL resolves symbols before passing forms to read-expr,
;; so (> :mass 4000) arrives with clojure.core/> instead of the symbol '>.
(def ^:private reverse-op-table
  {clojure.core/> '>
   clojure.core/< '<
   clojure.core/>= '>=
   clojure.core/<= '<=
   clojure.core/+ '+
   clojure.core/- '-
   clojure.core/* '*
   clojure.core// '/
   clojure.core/not 'not
   ;; datajure.expr stubs -> their canonical symbols
   ::sq 'sq
   ::log 'log
   ::and 'and
   ::or 'or
   ::not 'not})

(defn- ->op-sym
  "Normalise an op to its canonical symbol. Accepts symbols directly,
  or looks up fn/var values via reverse-op-table."
  [op]
  (if (symbol? op)
    op
    (clojure.core/or (reverse-op-table op)
                     (throw (ex-info "Unknown op in #dt/e expression — not a recognised symbol or fn"
                                     {:op op :op-type (type op)})))))

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
    (symbol? form) (lit-node form)
    (seq? form) (let [[op & args] form]
                  (op-node (->op-sym op) (mapv parse-form args)))
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
              op-fn (clojure.core/or (op-table op-sym)
                                     (throw (ex-info "Unknown op in #dt/e expression"
                                                     {:op op-sym})))
              arg-fns (mapv compile-expr (:op/args node))
              cmp? (comparison-ops op-sym)]
          (fn [ds]
            (let [args (map #(% ds) arg-fns)]
              (if (clojure.core/some nil? args)
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
  "Register the #dt/e reader tag via alter-var-root on *data-readers*.
  Fallback for AOT compilation or scripts where data_readers.clj is not picked
  up at startup. The primary registration mechanism is resources/data_readers.clj,
  which Clojure merges automatically for all threads at JVM startup."
  []
  (alter-var-root #'*data-readers* assoc 'dt/e #'read-expr))
