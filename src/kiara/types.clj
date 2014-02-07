(ns kiara.types
  "Schema type data"
  (:require [schema.core :as s])
  (:import [datomic.peer Connection]
           [java.net URI URL]))

(def DatomicUrlString
  (s/both String (s/pred #(= "datomic" (.getScheme (URI. %))) 'durl?)))

(def UriString (s/both String (s/pred #(instance? URI (URI. %)) 'uri?)))

(def Kiara
  {:system Connection
   :system-db DatomicUrlString
   :default Connection
   :default-db DatomicUrlString})
