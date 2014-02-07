(ns kiara.schema
  "Reads RDF and creates schema and data appropriate to Datomic"
  (:require [crg.turtle.parser :as p])
  (:import [java.util Date UUID]
           [java.net URI]
           [java.math BigDecimal BigInteger]
           [datomic Peer]))

(def super-types
  "Map Datomic types to a list of types that can store the same data without loss.
  :db.type/ref is a special case, as it refers to a Literal structure which can hold
  a type and any kind of value."
  {:db.type/string [:db.type/ref]
   :db.type/boolean [:db.type/ref]
   :db.type/long [:db.type/double :db.type/bigint :db.type/bigdec :db.type/ref]
   :db.type/bigint [:db.type/bigdec :db.type/ref]
   :db.type/float [:db.type/double :db.type/bigdec :db.type/ref]
   :db.type/double [:db.type/bigdec :db.type/ref]
   :db.type/bigdec [:db.type/ref]
   :db.type/ref [:db.type/ref]
   :db.type/instant [:db.type/ref]
   :db.type/uuid [:db.type/ref]
   :db.type/uri [:db.type/ref]
   :db.type/bytes [:db.type/ref]})

(defn set-conj
  "A conj function that creates a singleton set if there is nothing to conj to."
  [s v] (if s (conj s v) #{v}))

(def subsumptions
  "A map of types to the set of types that it can subsume. Built from super-types."
  (reduce
    (fn [m [k v]] (update-in m [k] set-conj v))
    {}
    (for [x (keys super-types) y (super-types x)] [y x])))

(defn subsumes
  "Tests if type a subsumes type b"
  [a b]
  (or (= a b) (contains? (subsumptions a) b)))

(defn least-common-type
  "For a pair of Datomic types, finds the least general type that can represent both types."
  [a b]
  (cond
    (subsumes a b) a
    (subsumes b a) b
    :default (let [ancestory (super-types a)]
               (loop [s (first ancestory) anc (rest ancestory)]
                 (if (nil? s)
                   :db.type/ref
                   (if (subsumes s b)
                     s
                     (recur (first anc) (rest anc))))))))

(def type-map
  "Maps data types to datomic types that can store them"
  {Long :db.type/long
   Integer :db.type/long
   Float :db.type/double
   Double :db.type/double
   Boolean :db.type/boolean
   BigDecimal :db.type/bigdec
   BigInteger :db.type/bigint
   String :db.type/string
   Date :db.type/instant
   UUID :db.type/uuid})

(defn type-for
  "Determines the datomic type that can refer to the given value."
  ([value]
   (type-map (type value) :db.type/ref))
  ([value existing]
   (least-common-type (type-for value) existing)))

(defn describe
  "Create a Datomic description for a predicate that matches a value, possibly expanding an existing description."
  ([predicate value]
   (when-not (keyword? predicate) (throw (IllegalArgumentException. "Only able to schematize predicates expressed as QNames")))
   {:db/id (Peer/tempid :db.part/db)
    :db/ident predicate
    :db/valueType (type-for value)
    :db/cardinality :db.cardinality/many
    :k/rdf true
    :db.install/_attribute :db.part/db})
  ([predicate value existing-desc]
   (update-in existing-desc [:db/valueType] type-for value)))

(defn domain-matches
  "Tests if a given predicate description describes a domain that matches the given value."
  [description value]
  (let [desc-type (:db/valueType description)
        value-type (type value)]
    (subsumes desc-type (type-map value-type))))

(defn add-predicate
  "Tests if a predicate is described. If not, then create a description and map the predicate to the description.
   If a description already exists, then check if the description needs updating, and modify it accordingly."
  [descriptions [_ predicate object]]
  (if-let [desc (descriptions predicate)]
    (if-not (domain-matches desc object)
      (assoc descriptions predicate (describe predicate object desc))
      descriptions)
    (assoc descriptions predicate (describe predicate object))))

(defn datomic-schema [i]
  (let [parser (p/create-parser i)
        triples (p/get-triples parser)]
    (vals (reduce add-predicate {} triples))))

