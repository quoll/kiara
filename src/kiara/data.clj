(ns kiara.data
  "Converts RDF into entity data for Datomic"
  (:require [datomic.api :refer [q] :as d]
            [crg.turtle.parser :as p])
  (:import [java.util Date UUID Map]
            [java.net URI]
            [java.math BigDecimal BigInteger]
            [datomic Peer]))

(defn namespace-data
  "Converts a namespace map into transaction data"
  [^Map prefixes]
  (letfn [(prefix-entity [[prefix namespace]]
            {:db/id (Peer/tempid :db.part/user)
             :k/prefix prefix
             :k/namespace namespace})])
    (map prefix-entity prefixes))

(defn encode-object
  "Converts an object into Object data for Datomic. A database value
  is supplied to allow schema lookups.
  Simple objects are returned as native data. Other objects get
  structured data. Returns a pair containing the Datomic value object
  along with a possible seq of other data that represents the object."
  [db o]
  ;; TODO - return the appropriate data
  [o nil]
  )

;; TODO - check if upsert will work in a single transaction (suspect not).
;; if not, then merge entities the same way that schema does
(defn triple-to-entity
  "Converts a triple into an RDF entity"
  [db triple]
  (let [subject (.getSubject triple)
        predicate (.getPredicate triple)
        object (.getObject triple)]
    (when-not (keyword? predicate)
      (throw (IllegalArgumentException. (str "Only QName predicates are supported: " predicate))))
    (let [[object-data extra-data] (encode-object db object)
          entity {:db/id (Peer/tempid :db.part/user)
                  predicate object-data}]
      (if (seq extra-data) (conj extra-data entity) [entity]))))

(defn datomic-data
  "Creates Datomic entity data that represents triples that can be found on a stream.
  A database is required to read the schema that will contain the triples.
  Returns the data stream and the parser. The parser is needed, since it will not
  contain the prefix map until the data stream has been processed."
  [database i]
  (let [parser (p/create-parser i)
        triples (p/get-triples parser)
        create-entity (partial triple-to-entity database)]
    [(mapcat create-entity triples) parser]))

