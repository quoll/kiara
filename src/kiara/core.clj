(ns kiara.core
  "Operations for initializing, loading, and querying a database.
  WARNING: No retrying or graceful fallbacks"
  (:require [kiara.core-schema :as cschema]
            [kiara.schema :as schema]
            [kiara.data :as data]
            [crg.turtle.qname :as qname]
            [clojure.java.io :as io]
            [clojure.string :as s]
            [datomic.api :refer [q] :as d])
  (:import [datomic Peer]))

(defn rdb
  "Gets the current db value on a connection. This may be expanded for reliability."
  [connection]
  (d/db connection))

(defn update-dbname
  "Updates the dbname in a Datomic database URI."
  [uri dbname]
  (condp re-matches uri
    #"datomic:ddb://([^/]*)/([^/]*)/([^?]*)(\\?.*)?" :>>
      (fn [[_ region table _ query]] (str "datomic:ddb://" region "/" table "/" dbname query))
    #"datomic:riak://([^/]*)/([^/]*)/([^?]*)(\\?.*)?" :>>
      (fn [[_ host bucket _ query]] (str "datomic:riak://" host "/" bucket "/" dbname query))
    #"datomic:couchbase://([^/]*)/([^/]*)/([^?]*)(\\?.*)?" :>>
      (fn [[_ host bucket _ query]] (str "datomic:couchbase://" host "/" bucket "/" dbname query))
    #"datomic:sql://([^?]*)?(\\?.*)?" :>>
      (fn [[_ _ jdbc-url]] (str "datomic:sql://" dbname jdbc-url))
    #"datomic:inf://([^/]*)/(.*)?" :>>
      (fn [[_ cluster-member-host _]] (str "datomic:inf://" cluster-member-host "/" dbname))
    #"datomic:dev://([^/]*)/(.*)?" :>>
      (fn [[_ transactor-host _]] (str "datomic:dev://" transactor-host "/" dbname))
    #"datomic:free://([^/]*)/(.*)?" :>>
      (fn [[_ transactor-host _]] (str "datomic:free://" transactor-host "/" dbname))
    #"datomic:mem://(.*)?" :>>
      (fn [[_ _]] (str "datomic:mem://" dbname))
    (throw (ex-info (str "Unknown Database URI form: " uri) {:uri uri}))))

(defn- biggest-ns-id
  [db]
  (let [prefixes (q '[:find ?p :where [?n :k/prefix ?p]] db)
        ids (map #(Integer/parseInt (subs (first %) 2)) prefixes)]
    (if (seq ids) (apply max ids) 0)))

(defn get-prefix
  "Get a new namespace prefix to use for a reference. If one does not exist, then generate a new one,
  and optimistically attempt to use it. The database function :atomic-check is used to check that the
  system table was not updated between scan and insertion."
  [sys p-ref]
  (let [sysdb (rdb sys)
        known-prefix (ffirst (q '[:find ?p :in $ ?pref :where [?n :k/prefix ?p][?n :k/namespace ?pref]] sysdb p-ref))]
    (or known-prefix
        (loop [sdb sysdb, new-ns-id (inc (biggest-ns-id sysdb))]
          (let [new-ns (str "ns" new-ns-id)
                latest (d/t->tx (d/basis-t sdb))
                [result error] (try [@(d/transact sys [[:atomic-check latest]
                                                       {:db/id (Peer/tempid :k/system)
                                                        :k/prefix new-ns
                                                        :k/namespace p-ref}]) nil]
                                    (catch Throwable t [nil t]))]
            (if result
              new-ns
              (let [new-sysdb (rdb sys)]
                (recur new-sysdb (inc (biggest-ns-id new-sysdb))))))))))

(defn known-prefixes
  "Returns a map of known prefixes to namespace references."
  [sys]
  (let [p (q '[:find ?p ?l :where [?n :k/prefix ?p] [?n :k/namespace ?l]] (rdb sys))]
    (into {} p)))

(defn generate-graph-uri
  "Creates a new graph URI, based on the system graph and a graph name"
  [k gn]
  (let [{sys :system, sysdb :system-db} k
        [p-ref local] (qname/split-iri gn)
        prefix (get-prefix sys p-ref)
        db-name (str prefix "-" local)]
    (update-dbname sysdb db-name)))

(defn get-default
  "Gets the default graph given the system graph, or a name. Only uses the name if the system graph has not yet been established."
  [system dn]
  (let [sysdb (rdb system)
        est (ffirst (q '[:find ?dn :where [?dg :k/db-name ?dn][?sys :k/default ?dg]] sysdb))
        defgraphname (or est dn)]
    (d/create-database defgraphname)
    defgraphname))

(defn get-graph
  "Gets a graph connection from a Kiara instance, creating a new one where necessary."
  [k gn]
  (let [sys (:system k)
        sysdb (rdb sys)
        established (ffirst (q '[:find ?dn :in $ ?gn :where [?g :k/db-name ?dn][?g :k/name ?gn]] sysdb gn))
        dbname (or established (generate-graph-uri k gn))]
    (d/create-database dbname)
    (let [conn (d/connect dbname)]
      (when-not established
        (d/transact conn cschema/core-attributes)
        (d/transact sys [{:db/id (Peer/tempid :k/system)
                          :k/name gn
                          :k/db-name dbname}]))
      conn)))

(def default-default-name "default")

(defn init
  "Creates a database and nitializes it. Receives a connection parameter"
  ([sys-db] (init sys-db (update-dbname sys-db default-default-name)))
  ([sys-db default-db]
   (let [created (d/create-database sys-db)
         created-default (d/create-database default-db)
         system-graph (d/connect sys-db)
         def-graph (d/connect default-db)]
     (when created
       @(d/transact system-graph cschema/system-partition)
       @(d/transact system-graph cschema/system-attributes)
       @(d/transact def-graph cschema/core-attributes))
     {:system system-graph
      :system-db sys-db
      :default def-graph
      :default-db (get-default system-graph default-db)})))

(def default-protocol "datomic:free")
(def default-host "localhost")
(def default-port 4334)
(def default-system "system")

(defn create
  "Set up a set of connections to databases that contain RDF graphs, starting with the system graph"
  ([] (create default-protocol default-host default-port default-system))
  ([root] (create root default-system))
  ([root system] (init (str root (if-not (= \/ (last root)) "/") system)))
  ([protocol host port system] (init (str protocol "://" host (if port (str ":" port)) "/" system))))

(defn infer-schema-to-conn
  "Reads a TTL file and infers a schema that will load it. Adds this schema to the connection."
  [c file]
  (with-open [f (io/input-stream file)]
    (d/transact c (schema/datomic-schema f)))
  c)

(defn load-schema
  "Infers a schema from TTL data in a file, and loads it into a Kiara instance"
  ([k file] (load-schema k file nil))
  ([k file graphname]
   (let [graph (if graphname (get-graph k graphname) (:default k))]
     (with-open [i (io/input-stream file)]
       (let [schema (schema/datomic-schema i)]
         (println "Schema: " schema)
         @(d/transact graph schema))))
   k))

(defn load-ttl
  "Load TTL data from a file into a Kiara instance."
  ([k file] (load-ttl k file nil))
  ([k file graphname]
   (let [graph (if graphname (get-graph k graphname) (:default k))]
     (with-open [f (io/input-stream file)]
       (let [sys (:system k)
             prefix-gen-fn (fn [_ namesp-ref] (get-prefix sys namesp-ref))
             [data parser] (data/datomic-data (rdb graph) f (known-prefixes sys) prefix-gen-fn)]
         @(d/transact graph data))))
   k))
