EARQ = ElasticSearch + ARQ 
==========================

EARQ is a combination of ARQ and ElasticSearch. It gives ARQ the ability to 
perform free text searches using an ElasticSearch cluster. ElasticSearch 
indexes are additional information for accessing the RDF graph, not storage 
for the graph itself.

This is *experimental* (and unsupported).


How to use it
-------------

This is how you build an index from a Jena Model:

    ModelIndexerString indexer = new ModelIndexerString("earq_index");
    indexer.indexStatements(model.listStatements());
    indexer.close();

This is how you configure ARQ to use ElasticSearch:
        
    IndexSearcher searcher = IndexSearcherFactory.create(Type.ELASTICSEARCH, "earq_index") ;
    EARQ.setDefaultIndex(searcher) ;

This is an example of a SPARQL query using the sarq:search property function: 

    PREFIX earq: <http://openjena.org/EARQ/property#>
    SELECT * WHERE {
        ?doc ?p ?lit .
        (?lit ?score ) earq:search "+text" .
    }


Acknowledgement
---------------
        
The design and part of the code has been inspired from LARQ, see:

 * [http://openjena.org/ARQ/lucene-arq.html](http://openjena.org/ARQ/lucene-arq.html)


Todo
----

 * ... still broken
 * Remove LARQ reference from ModelIndexerSubject
