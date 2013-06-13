(ns kiara.core
  "Operations for initializing, loading, and querying a database.
  WARNING: No retrying or graceful fallbacks"
  (:require [kiara.core-schema :as cschema]
            [kiara.schema :as schema]
            [kiara.data :as data]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [datomic.api :refer [q] :as d]))


(defn initialize
  "Creates a database and initializes it. Receives a connection parameter"
  [cn]
  (d/create-database cn)
  (let [c (d/connect cn)]
    (d/transact c cschema/core-attributes)
    c))

(defn init
  "Creates a database and initializes it. No params will build the database in memory,
  while a string will name the database. Valid URIs for datomic.api.connect may also be used."
  ([] (init (str "datomic:mem://kiara")))
  ([nm]
   (cond
     (map? nm) (initialize nm)
     (.startsWith (s/lower-case (str nm)) "datomic:") (initialize nm)
     :default (initialize (str "datomic:free://localhost/" nm)))))

(defn infer-schema
  "Reads a TTL file and infers a schema that will load it"
  [c file]
  (with-open [f (io/input-stream file)]
    (d/transact c (schema/datomic-schema f)))
  c)

(defn load-ttl
  "Loads TTL data from a file"
  [c file]
  (with-open [f (io/input-stream file)]
    (let [[data parser] (data/datomic-data nil f)]
      (pr data)
      (println)
      (let [f (d/transact c data)]
        (println @f))))
  c)

