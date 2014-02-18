(ns kiara.core-schema
  "Contains core schema for storing RDF in Datomic"
  (:require [datomic.api :refer [q] :as d]
            [datomic.function :as dfn])
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
    :db/ident :k/rdf
    :db/valueType :db.type/boolean
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Indicates that a property is used for RDF data."
    :db.install/_attribute :db.part/db}])

(def system-attributes
  "Attributes used in the system graph"
  [{:db/id (Peer/tempid :db.part/db)
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
    :db/ident :k/namespaces
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/many
    :db/doc "Refers to the namespaces for a graph."
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :sd/name
    :db/valueType :db.type/uri
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Graph name associated with a database/graph. See http://www.w3.org/ns/sparql-service-description#name"
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/db-name
    :db/valueType :db.type/uri
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Database name for holding a graph"
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :k/default
    :db/valueType :db.type/ref
    :db/cardinality :db.cardinality/one
    :db/index true
    :db/doc "Refers to the structure for the default database"
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :db.part/db)
    :db/ident :rdf/type
    :db/valueType :db.type/keyword
    :db/cardinality :db.cardinality/many
    :db/index true
    :db/doc "Describes the type of an entity"
    :db.install/_attribute :db.part/db}
   {:db/id (Peer/tempid :k/system)
    :db/ident :atomic-check
    :db/fn (dfn/construct {:lang "clojure"
                           :params '[db tx-id]
                           :code "(let [i (datomic.api/t->tx (datomic.api/basis-t db))] (if (= tx-id i) [] (throw (ex-info \"Transaction slip. Try again.\", {:expected tx-id :received i :retry true}))))"})}])

(def core-attributes "Declarations of all the attributes used by Kiara"
  (concat (map type-attr-struct type-attributes) internal-attributes))

(def system-partition "A partition for Kiara system attributes"
  [{:db/id (Peer/tempid :db.part/db)
    :db/ident :k/system
    :db.install/_partition :db.part/db}])

(defn filter-schema-tx
  "Filters a transaction that defines attributes to only include attributes not already in a database."
  [connection transaction-data]
  (let [existing-attributes (->> (q '[:find ?a :where [?e :db/ident ?a]] (d/db connection))
                                 (map first)
                                 set)]
    (remove #(existing-attributes (:db/ident %)) transaction-data)))

