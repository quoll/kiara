(ns kiara.test-load
  (:require [kiara.core-schema :as cschema]
            [kiara.schema :as schema]
            [kiara.data :as data]
            [kiara.core :as kiara]
            [clojure.java.io :as io]
            [datomic.api :refer [q] :as d])
  (:gen-class))

(defn -main
  "Load RDF into Datomic"
  [& args]
  (when-not (seq args)
    (println "Usage: kiara.test-load <ttl.file>")
    (System/exit 1))
  (let [data (-> (kiara/init)
                 (kiara/infer-schema (first args))
                 (kiara/load-ttl (first args))
                 d/db)]
    (pr (q '[:find ?subj ?pred ?o :where [?s ?p ?o][?s :db/ident ?subj][?p :db/ident ?pred]] data))
    (println)))

