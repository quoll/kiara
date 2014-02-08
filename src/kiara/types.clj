(ns kiara.types
  "Schema type data"
  (:require [schema.core :as s])
  (:import [datomic.peer Connection]
           [java.net URI URL]
           [java.util Date]))

(def DatomicUrlString
  (s/both String (s/pred #(= "datomic" (.getScheme (URI. %))) 'durl?)))

(def UriString (s/both String (s/pred #(instance? URI (URI. %)) 'uri?)))

(def UriString? (s/maybe UriString))

(s/defn prefix-style?
  "Tests if a parsed URI is a simple string with a trailing : character"
  [u]
  (= \: (nth u (dec (.length u)))))

(def PartialUriString
  (s/either (s/both String (s/pred prefix-style? 'prefix-style?))
            UriString))

(def Kiara
  {:system Connection
   :system-db DatomicUrlString
   :default Connection
   :default-db DatomicUrlString})

(def Subject (s/either long s/Keyword))

(def Literal (s/either long double String Date
                       {:value s/Any,
                        (s/optional-key :type) UriString}
                       {:value String,
                        (s/optional-key :lang) String}))

(def Resource (s/either Subject Literal))

(def Triple [(s/one Subject "s") (s/one s/Keyword "p") (s/one Resource "o")])
