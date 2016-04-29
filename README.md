# KIARA

A Clojure library to store RDF data in Datomic.

The principle here is to map the RDF data model onto Datomic's data model,
choosing Clojure types whenever possible to make it more idiomatic for Clojure,
and faster in Datomic..

This is still a work in progress. For the moment it reads RDF/Turtle files
and determines a schema that should accept the data. Uses CRG-Turtle to
load the RDF. Returns graphs as seqs of triples.

Kiara supports named graphs and a default graph. These are tracked in a system
graph, which records the graphs and their names, along with all the known
prefix/namespace-reference pairs.

Kiara is storing *all* IRIs as QNames (keywords internally). This is for
efficiency, and works much more elegantly with Clojure. When a prefix cannot be
determined for a namespace, a new one is generated. These prefixes are unique
across the system. It is possible to store URIs, so this may be made an option
in the future if it proves necessary.

Despite my best intentions, RDF/XML support will be required. The goal is to
support this natively, *without* resorting to Jena's parser.

## Usage

API usage only for the moment. See the test namespace for examples.

## TODO

* Flesh out tests for various RDF structures and large loads.
* Write an RDF/XML parser.
* Implement a rules engine, with flow control based on core.async.
* Provide a BGP resolution interface (for SPARQL engines).
* Create CLI tools.

## Name

Kiara Is A Recursive Acronym

## License

Copyright © 2013 Paula Gearon

Distributed under the Eclipse Public License, the same as Clojure.
