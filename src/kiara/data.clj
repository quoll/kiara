(ns kiara.data
  "Converts RDF into entity data for Datomic"
  (:require [datomic.api :refer [q] :as d]
            [crg.turtle.parser :as p])
  (:import [java.util Date UUID Map]
           [java.net URI]
           [java.math BigDecimal BigInteger]
           [crg.turtle.parser BlankNode Literal]
           [datomic Peer]))

(defn namespace-data
  "Converts a namespace map into transaction data"
  [^Map prefixes]
  (letfn [(prefix-entity [[prefix namespace]]
            {:db/id (Peer/tempid :db.part/user)
             :k/prefix prefix
             :k/namespace namespace})]
    (map prefix-entity prefixes)))

(defn encode-blank [{:keys [id]} bmap]
  (if-let [b (bmap id)]
    [b bmap nil]
    (let [b (Peer/tempid :db.part/user)]
      [b (assoc bmap id b) nil])))

(defprotocol ObjectEncodable
  (object-encode [db e bmap as] "Encodes a node into a structure suitable for Datomic"))

(defprotocol SubjectEncodable
  (subject-encode [e bmap] "Encodes a node into a structure suitable for Datomic"))

(extend-protocol ObjectEncodable
  Literal
  ;; TODO check the type works for t and lang, else exception.
  ;; Default requires check for string type.
  (object-encode [db {:keys [lex t lang]} bmap as]
    (cond
      t (let [node (Peer/tempid :db.part/user)]
          [node bmap {:db/id node, :k/value-s lex, :k/datatype t}])
      lang (let [node (Peer/tempid :db.part/user)]
            [node bmap {:db/id node, :k/value-s lex :k/lang lang}])
      :default [lex bmap nil]))
  BlankNode
  (object-encode [_ blank-node bmap as]
    (encode-blank blank-node bmap))
  Object
  (object-encode [db o bmap as] [o bmap nil])
  nil
  ;; TODO check that the type works, else promote
  (object-encode [db o bmap as] [o bmap nil]))

(extend-protocol SubjectEncodable
  Object
  (subject-encode [o bmap] [o bmap nil])
  BlankNode
  (subject-encode [blank-node bmap]
    (encode-blank blank-node bmap)))

(defn triple-to-entity
  "Converts a triple into an RDF entity"
  [db blank-map [subject predicate object]]
  (when-not (keyword? predicate)
    (throw (IllegalArgumentException. (str "Only QName predicates are supported: " predicate))))
  (let [[object-data blank-map other] (object-encode db object blank-map predicate)
        [subject-node blank-map _] (subject-encode subject blank-map)
        entity {:db/id (Peer/tempid :db.part/user)
                :k/id subject-node
                predicate object-data}]
    (if other
      [[entity other] blank-map]
      [[entity] blank-map])))

(defn datomic-data
  "Creates Datomic entity data that represents triples that can be found on a stream.
  A database is required to read the schema.
  Returns the data stream and the parser. The parser is needed, since it will not
  contain the prefix map until the data stream has been processed."
  [database i]
  (let [parser (p/create-parser i)
        triples (p/get-triples parser)
        add-entity (fn [[s m] triple]
                     (let [[entities new-map] (triple-to-entity database m triple)]
                       [(concat s entities) new-map]))]
    [(first (reduce add-entity [[] {}] triples)) parser]))

