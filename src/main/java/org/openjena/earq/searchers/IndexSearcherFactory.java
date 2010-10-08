package org.openjena.earq.searchers;

import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;
import org.openjena.earq.IndexSearcher;

public class IndexSearcherFactory {

	public static IndexSearcher create (EARQ.Type type, String location) {
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
	
}
