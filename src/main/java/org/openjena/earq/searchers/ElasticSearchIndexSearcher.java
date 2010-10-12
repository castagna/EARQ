/*
 * Copyright © 2010 Talis Systems Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.openjena.earq.searchers;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.fieldQuery;

import java.util.Iterator;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.openjena.earq.Document;
import org.openjena.earq.EARQ;
import org.openjena.earq.ElasticSearchConstants;
import org.openjena.earq.IndexSearcher;

import com.hp.hpl.jena.util.iterator.Map1;
import com.hp.hpl.jena.util.iterator.Map1Iterator;

public class ElasticSearchIndexSearcher extends IndexSearcherBase implements IndexSearcher {
	
	public final static int NUM_RESULTS = 10000;
	
	private Client client = null;
	private final String index;
	
	public ElasticSearchIndexSearcher(String index) {
    	Node node = NodeBuilder.nodeBuilder()
			.client(true)
			.loadConfigSettings(false)
			.clusterName(ElasticSearchConstants.CLUSTER_NAME)
			.local(ElasticSearchConstants.LOCAL)
			.settings(
				ImmutableSettings.settingsBuilder()
//    				.put("network.host", "127.0.0.1")
					.put("index.store.type", "memory")
					.put("gateway.type", "none")
					.put("index.number_of_shards", 1)
					.put("index.number_of_replicas", 1).build()
			).node().start();
    	this.client = node.client();
    	this.index = index;
	}

	public ElasticSearchIndexSearcher(Client client, String index) {
		this.client = client;
		this.index = index;
	}
	
	@Override
    public Iterator<Document> search(String query) {
    	SearchRequestBuilder srb = client.prepareSearch(index);
    	srb.setSearchType(SearchType.QUERY_AND_FETCH);
    	srb.setQuery(fieldQuery(EARQ.fText, query));
//    	srb.addField(EARQ.fURI);
//    	srb.addField(EARQ.fBNodeID);
//    	srb.addField(EARQ.fLex);
//    	srb.addField(EARQ.fDataType);
//    	srb.addField(EARQ.fLang);
    	srb.setFrom(0);
    	srb.setSize(NUM_RESULTS);
    	srb.setExplain(false);
    	SearchResponse response = srb.execute().actionGet();
    	SearchHits sh = response.getHits();
    	return new Map1Iterator<SearchHit, Document>(new SearchHit2Document(), sh.iterator());
    }
    
	class SearchHit2Document implements Map1<SearchHit, Document> {
		@Override
		public Document map1(SearchHit hit) {
			Document doc = new Document();
			doc.set(EARQ.fId, hit.getId());
			doc.set(EARQ.fScore, String.valueOf(hit.score()));
			Map<String, Object> source = hit.getSource();
			for (String name : source.keySet()) {
				doc.set(name, source.get(name).toString());
			}
//			Map<String, SearchHitField> fields = hit.getFields();
//			for (String name : fields.keySet()) {
//				doc.set(name, fields.get(name).getValue().toString());
//			}
			return doc;
		}
	}
	
}