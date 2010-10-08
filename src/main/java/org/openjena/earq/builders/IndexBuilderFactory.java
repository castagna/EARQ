package org.openjena.earq.builders;

import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;

public class IndexBuilderFactory {

	public static IndexBuilderBase create(EARQ.Type type, String location) {
		switch (type) {
		case LUCENE:
			return new LuceneIndexBuilder(location);
		case SOLR:
			return new SolrIndexBuilder(location);
		case ELASTICSEARCH:
			return new ElasticSearchIndexBuilder(location);
		default:
			throw new EARQException("Unknown index type.");
		}
	}
	
}
