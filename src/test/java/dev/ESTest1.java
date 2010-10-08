package dev;

import static org.elasticsearch.index.query.xcontent.QueryBuilders.fieldQuery;

import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.index.IndexRequestBuilder;
import org.elasticsearch.client.action.search.SearchRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ESTest1 extends Assert {

	private Node node;
	
    private static final String INDEX_NAME = "test_index";
    private static final String DOC_ID = "1";
    private static final String FIELD_NAME = "title";
    private static final String FIELD_VALUE = "Hello, World!";

    @Before public void startNode() {
    	node = NodeBuilder.nodeBuilder()
    		.loadConfigSettings(false)
    		.clusterName("mycluster")
    		.local(true)
    		.settings(ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
    			.put("index.number_of_shards", 1)
    			.put("index.number_of_replicas", 1).build()
    		).node().start();
    }
    
    @After public void stopNode() {
    	node.stop();
    }
    
    @Test
    public void test() throws IOException {
    	Client client = node.client();

    	IndexResponse res = index(client);
    	assertEquals(DOC_ID, res.id());
    	refresh(client);
    	SearchHits hits = search(client, "Hello");
    	assertEquals(1, hits.totalHits());
    	
    	System.out.println(hits.getAt(0));
    	System.out.println(hits.getHits()[0]);
    	System.out.println(hits.getHits()[0].getSource());
    	System.out.println(hits.getHits()[0].getFields());
    	
    	System.out.println(hits.getAt(0).getFields());
    	System.out.println(hits.getAt(0).getFields().get(FIELD_NAME));
    	System.out.println(hits.getAt(0).getFields().get(FIELD_NAME).value());
    	
    	assertEquals(FIELD_VALUE, hits.getAt(0).getFields().get(FIELD_NAME).value());
    }

    private void refresh(Client client) {
    	client.admin().indices().refresh(refreshRequest()).actionGet();
    	client.admin().cluster().prepareHealth().setWaitForYellowStatus().setTimeout("10s").execute().actionGet();
    }

    private SearchHits search(Client client, String query) {
    	SearchRequestBuilder srb = client.prepareSearch(INDEX_NAME);
    	srb.setSearchType(SearchType.DFS_QUERY_THEN_FETCH);
    	srb.setQuery(fieldQuery(FIELD_NAME, query));
    	srb.setFrom(0);
    	srb.setSize(10);
    	srb.setExplain(false);
    	SearchResponse response = srb.execute().actionGet();
    	return response.getHits();
    }

    private IndexResponse index(Client client) throws IOException {
		IndexRequestBuilder irb = client.prepareIndex(INDEX_NAME, "node", DOC_ID);
		XContentBuilder cb = jsonBuilder();
		cb.startObject();
		cb.field(FIELD_NAME, FIELD_VALUE);
		cb.endObject();
		irb.setSource(cb);
		irb.setCreate(true);
		return irb.execute().actionGet();
    }

} 