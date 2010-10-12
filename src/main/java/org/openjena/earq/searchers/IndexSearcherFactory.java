package org.openjena.earq.searchers;

import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;
import org.openjena.earq.IndexBuilder;
import org.openjena.earq.IndexSearcher;
import org.openjena.earq.builders.ElasticSearchIndexBuilder;
import org.openjena.earq.builders.LuceneIndexBuilder;
import org.openjena.earq.builders.SolrIndexBuilder;

public class IndexSearcherFactory {

	public static IndexSearcher create ( EARQ.Type type, String location ) {
		switch (type) {
		case LUCENE:
			return new LuceneIndexSearcher(location);
		case SOLR:
			return new SolrIndexSearcher(location);
		case ELASTICSEARCH:
			return new ElasticSearchIndexSearcher(location);
		default:
			throw new EARQException("Unknown index type.");
		}
	}

	public static IndexSearcher create ( IndexBuilder builder ) {
		if ( builder instanceof ElasticSearchIndexBuilder ) {
			ElasticSearchIndexBuilder esib = (ElasticSearchIndexBuilder)builder;
			return new ElasticSearchIndexSearcher(esib.getClient(), esib.getIndexName());
		} else if ( builder instanceof LuceneIndexBuilder ) {
			LuceneIndexBuilder lib = (LuceneIndexBuilder)builder;
			return new LuceneIndexSearcher(lib.getDirectory());
		} else if ( builder instanceof SolrIndexBuilder ) {
			// TODO
			// SolrIndexBuilder sib = (SolrIndexBuilder)builder;
		} 
		
		throw new EARQException("Unknown index type.");
	}
	
}
