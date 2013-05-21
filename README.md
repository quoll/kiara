# KIARA

Kiara Is A Recursive Acronym

A Clojure library to load RDF data into Datomic.

This is still a work in progress. For the moment it reads RDF/Turtle files
and determines a schema that should accept the data. Uses CRG-Turtle to
load the RDF.

Kiara now defines the schema, and writes mostly conforming data structures.
Multi-type predicates are correctly handled in the schema, but not yet done
in the data structures. At the moment, the only use of this is in the test
printing stub. Once full typing is handled then it can be integrated with
code that calls datomic. This code should also introduce a database for
holding database meta-data (namespace prefixes, etc).

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
