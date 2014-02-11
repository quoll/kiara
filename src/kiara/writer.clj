(ns kiara.writer
  "Function for writing graphs in various formats"
  (:require [clojure.string :as string]
            [clojure.set :as set]
            [kiara.core :as k]
            [kiara.types :refer [Kiara UriString?]]
            [datomic.api :refer [q] :as d]
            [schema.core :as s])
  (:import [java.net URI]
           [datomic.db Db]
           [datomic.query EntityMap]))

(s/defn ttl-prefixes :- [String]
  "Returns the prefixes of a namespace in TTL format"
  [namespaces :- {String URI}]
  (map (fn [[p n]] (str "@prefix " p ": <" n "> .")) namespaces))

(s/defn ttl-properties :- String
  "Returns the property/value strings for a subject"
  [s :- EntityMap, props]
  (string/join "; " (map (fn [p] (str p " " (string/join ", " (get s p))))
                      props)))

(s/defn ttl-data :- [String]
  "Returns the data, one subject at a time, in TTL format"
  [graph :- Db, subjects :- [long]]
  (let [props (into #{} (map first (q '[:find ?pn :where [?p :k/rdf] [?p :db/ident ?pn]] graph)))]
    (map (fn [s]
           (let [subj (d/entity graph s)]
             (str (:db/ident subj) " "
                  (ttl-properties subj (filter props (keys subj))) ".")))
         subjects)))

(s/defn write-ttl :- [String]
  "Writes a graph as Turtle"
  [{system :system :as k} :- Kiara, graph-name :- UriString?]
  (let [sys (k/rdb system)
        graph (k/get-graph k graph-name)
        namespaces (into {} (q '[:find ?p ?n
                                 :in $ ?gn
                                 :where [?g :sd/name ?gn] [?g :k/namespaces ?ns]
                                 [?ns :k/prefix ?p] [?ns :k/namespace ?n]]
                               sys (URI. graph-name)))
        subjects (map first (q '[:find ?s :where [?p :k/rdf] [?s ?p]] graph))]
    (concat (ttl-prefixes namespaces) [""]
            (ttl-data graph subjects))))

