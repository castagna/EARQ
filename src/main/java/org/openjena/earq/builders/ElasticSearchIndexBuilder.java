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

package org.openjena.earq.builders;

import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.delete.DeleteRequestBuilder;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.openjena.earq.Document;
import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;
import org.openjena.earq.ElasticSearchConstants;
import org.openjena.earq.IndexSearcher;
import org.openjena.earq.searchers.IndexSearcherFactory;

public class ElasticSearchIndexBuilder extends IndexBuilderBase {

	private Node node;
	private Client client;
	private final String index;
	
	public ElasticSearchIndexBuilder(String index) { 
    	super() ; 

    	node = NodeBuilder.nodeBuilder()
    		.client(true)
    		.loadConfigSettings(false)
    		.clusterName(ElasticSearchConstants.CLUSTER_NAME)
    		.local(ElasticSearchConstants.LOCAL)
    		.settings(
    			ImmutableSettings.settingsBuilder()
        			.put("network.host", "127.0.0.1")
//    				.put("index.store.type", "memory")
    				.put("gateway.type", "none")
    				.put("index.number_of_shards", 1)
    				.put("index.number_of_replicas", 1).build()
    		).node().start();
        this.client = node.client();
    	this.index = index;

    	try {
			createMapping();
    	} catch (Exception e ) {
    		e.printStackTrace();
    		// TODO: add loging
		}
    }
	
	public Client getClient() {
		return client;
	}
	
	public String getIndexName() {
		return index;
	}

	private CreateIndexResponse createMapping() throws IOException {
		XContentBuilder mapping = jsonBuilder().startObject().startObject("properties")
        .startObject(EARQ.fText).field("type", "string").field("index", "analyzed").field("store", "no").endObject()
        .startObject(EARQ.fLex).field("type", "string").field("index", "no").field("store", "yes").endObject()
        .startObject(EARQ.fDataType).field("type", "string").field("index", "no").field("store", "yes").endObject()
        .startObject(EARQ.fLang).field("type", "string").field("index", "no").field("store", "yes").endObject()
        .startObject(EARQ.fBNodeID).field("type", "string").field("index", "no").field("store", "yes").endObject()
        .startObject(EARQ.fURI).field("type", "string").field("index", "no").field("store", "yes").endObject()
        .endObject().endObject();
		return client.admin().indices().create(createIndexRequest(index).mapping(ElasticSearchConstants.INDEX_TYPE, mapping)).actionGet();
	}

	@Override
	public void add(Document doc) {
		try {
			IndexRequestBuilder irb = client.prepareIndex(index, ElasticSearchConstants.INDEX_TYPE, doc.get(EARQ.fId));
			XContentBuilder cb = jsonBuilder();
			cb.startObject();
			for ( String name : doc.getNames() ) {
				if ( ! name.equals(EARQ.fId) ) {
					cb.field(name, doc.get(name));
				}
			}
			cb.endObject();
			irb.setSource(cb);
			/* IndexResponse response = */ irb.execute().actionGet();
		} catch (ElasticSearchException e) {
			throw new EARQException(e.getMessage(), e);
		} catch (IOException e) {
			throw new EARQException(e.getMessage(), e);
		}
	}

	@Override
	public void delete(String id) {
		DeleteRequestBuilder drb = client.prepareDelete(index, ElasticSearchConstants.INDEX_TYPE, id);
		drb.setRefresh(true);
		/* DeleteResponse response = */ drb.execute().actionGet();
	}

	@Override
	public IndexSearcher getIndexSearcher() {
		return IndexSearcherFactory.create(this);
	}

	@Override
	public void close() {
//		optimize();
		// TODO: why if I do not call refresh() and I call node.close() all my tests fail?
		refresh();
//		node.close();
	}

	private void refresh() {
		client.admin().indices().prepareRefresh(index).execute().actionGet();
	}
//	
//	private void optimize() {
//		AdminClient ac = client.admin();
//		OptimizeRequestBuilder orb = ac.indices().prepareOptimize(index);
//		/* OptimizeResponse response = */ orb.execute();
//	}

}
