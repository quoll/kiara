# KIARA

Kiara Is A Recursive Acronym

A Clojure library to load RDF data into Datomic.

This is still a work in progress. For the moment it reads RDF/Turtle files
and determines a schema that should accept the data. Uses CRG-Turtle to
load the RDF.

Lots of breaking changes, but new features. Not building right now, but this
is changing quickly.

Now defines a system graph, which records the known graphs in the system,
the name of the default graph, and all the known prefix/namespace-reference pairs.

While prefix/namespace-reference pairs should be stored on a graph-by-graph
basis (strictly speaking, on a per-load basis), this is generally unnecessary
in practice.

Also moving to storing *all* IRIs as QNames (keywords internally). This is for
efficiency, and works much more elegantly with Clojure. When a prefix cannot be
determined for a namespace, a new one is generated. These prefixes are unique
across the system.

Cleanup will get back to Kiara defining the schema correctly, and we are about
to load the data into Datomic. Once this is running correctly, we can look to
export RDF as well.

Despite my best intentions, RDF/XML support will be required. The goal is to
support this natively, *without* resorting to Jena's parser.

## Usage

Not running right now...

There is a test stub to run the schema inferencer and print.
When working, this can be run with:

    $ lein uberjar
    $ java -jar target/kiara-0.1.0-SNAPSHOT-standalone.jar data.ttl

## License

Copyright © 2013 Paul Gearon

Distributed under the Eclipse Public License, the same as Clojure.
