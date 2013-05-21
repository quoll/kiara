(ns kiara.core-schema
  "Contains core schema for storing RDF in Datomic"
  (:require [datomic.api :refer [q] :as d])
  (:import [java.util Date UUID]
           [java.net URI]
           [java.math BigDecimal BigInteger]
           [datomic Peer]))

(def type-attributes
  "Maps datomic types to properties that can be used by collections to refer to those types"
  {:db.type/ref :k/value
   :db.type/string :k/value-s
   :db.type/boolean :k/value-b
   :db.type/long :k/value-l
   :db.type/bigint :k/value-bi
   :db.type/float :k/value-f
   :db.type/double :k/value-d
   :db.type/bigdec :k/value-bd
   :db.type/instant :k/value-dt
   :db.type/uuid :k/value-uu
   :db.type/uri :k/value-u})

(defn- type-attr-struct
  "Converts a type/name pair into a declaration for an attribute."
  [[t p]]
  (let [attr {:db/id (Peer/tempid :db.part/db)
              :db/ident p
              :db/valueType t
              :db/cardinality :db.cardinality/one
              :db.install/_attribute :db.part/db}]
    (if (= t :db.type/string) (assoc attr :db/fulltext true) attr)))

(def ^:private internal-attributes
  "Attributes used internally by Kiara"
  [{:db/id (Peer/tempid :db.part/db)
    :db/ident :k/id
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true
    :db/doc "QName/keyword value for subjects. Preferred over URIs when possible."
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/uid
    :db/valueType :db.type/uri
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true
    :db/doc "URI value for subjects. If the URI can be expressed as a QName then use :/id instead."
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/prefix
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/index true
    :db/doc "Prefix that identifies a namespace."
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/namespace
    :db/valueType :db.type/uri
    :db/cardinality :db.cardinality/one
    :db/unique :db.unique/identity
    :db/doc "Namespace associated with a prefix."
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/lang
    :db/valueType :db.type/string
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Language code associated with a string literal"
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/datatype
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Datatype for a literal. Refers to a QName/keyword."
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/datatype-u
    :db/valueType :db.type/uri
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Datatype for a literal as a full URI. Prefer :k/datatype where possible."
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/name
    :db/valueType :db.type/uri
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Graph name associated with a database"
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/db-name
    :db/valueType :db.type/uri
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Database name for holding a graph"
    :db.install/_attribute :db.part/db}])

(def core-attributes "Declarations of all the attributes used by Kiara"
  (concat (map type-attr-struct type-attributes) internal-attributes))

(defn filter-schema-tx
  "Filters a transaction that defines attributes to only include attributes not already in a database."
  [connection transaction-data]
  (let [existing-attributes (->> (q '[:find ?a :where [?e :db/ident ?a]] (d/db connection))
                                 (map first)
                                 set)]
    (remove #(existing-attributes (:db/ident %)) transaction-data)))

