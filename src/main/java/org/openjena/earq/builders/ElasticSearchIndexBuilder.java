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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.client.action.admin.indices.refresh.RefreshRequestBuilder;
import org.elasticsearch.client.action.delete.DeleteRequestBuilder;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.openjena.earq.Document;
import org.openjena.earq.EARQ;
import org.openjena.earq.EARQException;
import org.openjena.earq.IndexSearcher;
import org.openjena.earq.searchers.ElasticSearchIndexSearcher;

public class ElasticSearchIndexBuilder extends IndexBuilderBase {

	private Node node = null;
	private Client client = null;
	private final String index;
	
	public ElasticSearchIndexBuilder(String index) { 
    	super() ; 

    	node = NodeBuilder.nodeBuilder().node().start();
    	client = node.client();
    	this.index = index;
    }

	@Override
	public void add(Document doc) {
		try {
			IndexRequestBuilder irb = client.prepareIndex(index, "node", doc.get(EARQ.fId));
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
		DeleteRequestBuilder drb = client.prepareDelete(index, "node", id);
		drb.setRefresh(true);
		/* DeleteResponse response = */ drb.execute().actionGet();
	}

	@Override
	public IndexSearcher getIndexSearcher() {
		return new ElasticSearchIndexSearcher(node, index);
	}

	@Override
	public void close() {
//		optimize();
//		refresh();
		node.close();
	}

	private void refresh() {
		AdminClient ac = client.admin();
		RefreshRequestBuilder rrb = ac.indices().prepareRefresh(index);
		rrb.execute();
	}
	
	private void optimize() {
		AdminClient ac = client.admin();
		OptimizeRequestBuilder orb = ac.indices().prepareOptimize(index);
		/* OptimizeResponse response = */ orb.execute();
	}

}
