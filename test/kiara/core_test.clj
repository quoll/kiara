(ns kiara.core-test
  (:require [kiara.util :as ku]
            [kiara.writer :as w]
            [schema.test])
  (:use clojure.test
        kiara.core))

(use-fixtures :once schema.test/validate-schemas)

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

(deftest load-default
  (testing "Load a simple graph"
    (let [k (create)
          k (load-schema k (ku/stream simple-data))
          k (load-ttl k (ku/stream simple-data))
          t (get-triples k)]
      (is (= 1 (count t)))
      (is (= [:a :b 1.0] (first t))))))

(deftest load-graph
  (testing "Load a named graph"
    (let [k (create)
          k (load-schema k (ku/stream simple-data) "my:graph")
          k (load-ttl k (ku/stream simple-data) "my:graph")
          t (get-triples k "my:graph")]
      (is (= 1 (count t)))
      (is (= [:a :b 1.0] (first t))))))

(deftest multi-load
  (testing "Load a named graph more than once"
    (let [k (create)
          tf (fn [] (let [k (load-schema k (ku/stream simple-data) "my:graph")
                         k (load-ttl k (ku/stream simple-data) "my:graph")
                         t (get-triples k "my:graph")]
                     (is (= 1 (count t)))
                     (is (= [:a :b 1.0] (first t)))))]
      (tf)
      (tf))))

(deftest write-ttl-default
  (testing "Simple TTL output from default"
    (let [k (create)
          k (load-schema k (ku/stream simple-data))
          k (load-ttl k (ku/stream simple-data))
          [prefixes triples :as s] (w/write-ttl k)]
      (is (= 2 (count s)))
      (is (= "@prefix : <http://example.org#> ." (first prefixes)))
      (is (= ":a :b 1.0 ." (first triples))))))

(deftest write-ttl
  (testing "Simple TTL output"
    (let [k (create)
          k (load-schema k (ku/stream simple-data) "my:graph")
          k (load-ttl k (ku/stream simple-data) "my:graph")
          [prefixes triples :as s] (w/write-ttl k "my:graph")]
      (is (= 2 (count s)))
      (is (= "@prefix : <http://example.org#> ." (first prefixes)))
      (is (= ":a :b 1.0 ." (first triples))))))
