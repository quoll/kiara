(ns kiara.print
  (:require [kiara.core :as k]
            [clojure.java.io :as io])
  (:gen-class))

(defn -main
  "Load RDF and print the schema"
  [& args]
  (when-not (seq args)
    (println "Usage: kiara.print <ttl.file>")
    (System/exit 1))
  (with-open [f (io/input-stream (first args))]
    (prn (k/datomic-schema f))))
