(ns kiara.core-test
  (:require [kiara.util :as ku])
  (:use clojure.test
        kiara.core))

(deftest init-test
  (testing "Initialize the system. No testing, just avoid exceptions"
    (let [k1 (create)
          k2 (create "datomic:free://localhost:4334/")]
      (is (= (:system-db k1) "datomic:free://localhost:4334/system"))
      (is (= (:system-db k2) "datomic:free://localhost:4334/system"))
      (is (= (:default-db k1) "datomic:free://localhost:4334/default"))
      (is (= (:default-db k2) "datomic:free://localhost:4334/default")))))

(def simple-data
"@prefix : <http://example.org#> .
:a :b 1.0 .")

(deftest load-test
  (testing "Load a simple graph"
    (let [k (create)
          k (load-schema k (ku/stream simple-data))
          k (load-ttl k (ku/stream simple-data))]
      )))
