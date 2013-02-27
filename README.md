# KIARA

Kiara Is A Recursive Acronym

A Clojure library to load RDF data into Datomic.

This is still a work in progress. For the moment it reads RDF/Turtle files
and determines a schema that should accept the data.

Very soon the core schema will be available (in particular, how to store Literals
that don't conform to standard Clojure types), along with code to insert the RDF
into the schema that has been built.

The next steps will be to export RDF from Datomic, and then to start supporting
SPARQL querying.

## Usage

For the moment, there is a test stub to run the schema inferencer and print.
This can be run with:

  $ lein uberjar
  $ java -jar target/kiara-0.1.0-SNAPSHOT-standalone.jar data.ttl

## License

Copyright © 2013 Paul Gearon

Distributed under the Eclipse Public License, the same as Clojure.
