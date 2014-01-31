(ns kiara.util
  (:require [clojure.java.io :as io])
  (:import [java.io ByteArrayInputStream]
           [java.nio.charset Charset]))

(def utf8charset (Charset/forName "UTF-8"))

(defn stream
  [^String s]
  (ByteArrayInputStream. (.getBytes s utf8charset)))
