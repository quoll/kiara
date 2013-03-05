(ns kiara.print
  (:require [kiara.schema :as schema]
            [clojure.java.io :as io])
  (:gen-class))

(defn -main
  "Load RDF and print the schema"
  [& args]
  (when-not (seq args)
    (println "Usage: kiara.print <ttl.file>")
    (System/exit 1))
  (with-open [f (io/input-stream (first args))]
    (prn (schema/datomic-schema f))))
