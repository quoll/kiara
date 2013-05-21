(ns kiara.print
  (:require [kiara.core-schema :as cschema]
            [kiara.schema :as schema]
            [kiara.data :as data]
            [clojure.java.io :as io])
  (:gen-class))

(defn -main
  "Load RDF and print the schema"
  [& args]
  (when-not (seq args)
    (println "Usage: kiara.print <ttl.file>")
    (System/exit 1))
  (prn "CORE:")
  (prn cschema/core-attributes)
  (println)
  (with-open [f (io/input-stream (first args))]
    (prn (schema/datomic-schema f)))
  (with-open [f (io/input-stream (first args))]
    (prn (first (data/datomic-data nil f)))))
