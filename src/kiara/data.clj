(ns kiara.data
  "Converts RDF into entity data for Datomic"
  (:require [datomic.api :refer [q] :as d]
            [crg.turtle.parser :as p])
  (:import [java.util Date UUID Map]
           [java.net URI]
           [java.math BigDecimal BigInteger]
           [crg.turtle.parser BlankNode Literal]
           [datomic Peer]
           [datomic.db DbId]))

(defn namespace-data
  "Converts a namespace map into transaction data"
  [^Map prefixes]
  (letfn [(prefix-entity [[prefix namespace]]
            {:db/id (Peer/tempid :db.part/user)
             :k/prefix prefix
             :k/namespace namespace})]
    (map prefix-entity prefixes)))

(defn encode-blank
  "Tests if a blank node is known, and returns it if so. Otherwise, add a new node
  to the map and return that."
  [{:keys [id]} bmap]
  (if-let [b (bmap id)]
    [b bmap nil]
    (let [b (Peer/tempid :db.part/user)]
      [b (assoc bmap id b) nil])))

(defprotocol ObjectEncodable
  (object-encode [e db bmap as] "Encodes a node into a structure suitable for Datomic"))

(defprotocol SubjectEncodable
  (subject-encode [e bmap] "Encodes a node into a structure suitable for Datomic")
  (subject-entity [s p o] "Creates an entity for a subject node"))

(extend-protocol ObjectEncodable
  Literal
  ;; TODO check the type works for t and lang, else exception.
  ;; Default requires check for string type.
  (object-encode [{:keys [lex t lang]} db bmap as]
    (println "encoding literal: " lex)
    (cond
      t (let [node (Peer/tempid :db.part/user)]
          [node bmap {:db/id node, :k/value-s lex, :k/datatype t}])
      lang (let [node (Peer/tempid :db.part/user)]
            [node bmap {:db/id node, :k/value-s lex :k/lang lang}])
      :default [lex bmap nil]))
  BlankNode
  (object-encode [blank-node db bmap as]
    (encode-blank blank-node bmap))
  Object
  ;; TODO check that the type works, else promote
  (object-encode [o db bmap as] [o bmap nil])
  nil
  (object-encode [o db bmap as] [o bmap nil]))

(extend-protocol SubjectEncodable
  Object    ;; passthrough for encoding, expected to be keyword for entity encoding
  (subject-encode [o bmap] [o bmap nil])
  (subject-entity [s p o] {:db/id (Peer/tempid :db.part/user), :db/ident s, p o})

  URI    ;; not expected. Hopefully all are converted to keyword
  (subject-encode [u bmap] [u bmap nil])
  (subject-entity [s p o] {:db/id (Peer/tempid :db.part/user), :k/uid s, p o})

  BlankNode  ;; only for encoding. Entity generation is an error
  (subject-encode [blank-node bmap] (encode-blank blank-node bmap))
  (subject-entity [s p o] (throw (ex-info "Bad conversion of blank node to entity" s)))

  DbId    ;; not expected for encoding, just entity generation
  (subject-encode [n bmap] [n bmap nil])
  (subject-entity [s p o] {:db/id s, p o}))

(defn triple-to-entity
  "Converts a triple into an RDF entity"
  [db blank-map [subject predicate object]]
  (when-not (keyword? predicate)
    (throw (IllegalArgumentException. (str "Only QName predicates are supported: " predicate))))
  (let [[object-data blank-map other] (object-encode object db blank-map predicate)
        [subject-node blank-map _] (subject-encode subject blank-map)
        entity (subject-entity subject-node predicate object-data)]
    (if other
      [[entity other] blank-map]
      [[entity] blank-map])))

(defn datomic-data
  "Creates Datomic entity data that represents triples that can be found on a stream.
  A database is required to read the schema.
  Returns the data stream and the parser. The parser is needed, since it will not
  contain the prefix map until the data stream has been processed."
  [database i known-ns]
  (let [parser (p/create-parser i known-ns)
        triples (p/get-triples parser)
        add-entity (fn [[s m] triple]
                     (let [[entities new-map] (triple-to-entity database m triple)]
                       [(concat s entities) new-map]))]
    [(first (reduce add-entity [[] {}] triples)) parser]))

(defn namespace-data
  "Creates a transaction data for a namespace, based on what a parser returns"
  [^crg.turtle.parser.TParser parser ^Long graph-eid]
  (let [prefix-map (p/get-prefix-map parser)
        ns-entities (map (fn [[p n]] {:db/id (Peer/tempid :k/system)
                                     :k/prefix p
                                     :k/namespace (URI. n)})
                         prefix-map)]
    [{:db/id graph-eid
      :k/namespaces ns-entities}]))

