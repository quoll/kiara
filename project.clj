(defproject kiara "0.1.0-SNAPSHOT"
  :description "RDF storage in Datomic"
  :url "http://github.com/quoll"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojars.quoll/turtle "0.2.1"]
                 [com.datomic/datomic-free "0.9.4556"]
                 [prismatic/schema "0.2.0"]]
  :main kiara.test-load
  
  :profiles {:test {:jvm-opts ["-Xms96m" "-Xmx1g" "-Ddatomic.objectCacheMax=128M" "-Ddatomic.memoryIndexMax=64M"]}}
  
  :jvm-opts ["-Xms96m" "-Xmx1g" "-Ddatomic.objectCacheMax=128m" "-Ddatomic.memoryIndexMax=64m"])
