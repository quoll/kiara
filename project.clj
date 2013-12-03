(defproject kiara "0.1.0-SNAPSHOT"
  :description "RDF storage in Datomic"
  :url "http://github.com/quoll"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojars.quoll/turtle "0.1.4"]
                 [com.datomic/datomic-free "0.9.4324"]]
  :main kiara.test-load)
