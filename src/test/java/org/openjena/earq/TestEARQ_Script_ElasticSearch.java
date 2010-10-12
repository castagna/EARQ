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

package org.openjena.earq;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

public class TestEARQ_Script_ElasticSearch extends TestEARQ_Script {

    private static Node node = null;
    private Client client = null;
    
    @BeforeClass public static void startCluster() {
    	EARQ.TYPE = EARQ.Type.ELASTICSEARCH; 
    	location = "test";

    	node = NodeBuilder.nodeBuilder().loadConfigSettings(false).clusterName(ElasticSearchConstants.CLUSTER_NAME).local(ElasticSearchConstants.LOCAL).settings(
				ImmutableSettings.settingsBuilder()
//					.put("network.host", "127.0.0.1")
//					.put("index.store.type", "memory")
					.put("gateway.type", "none")
					.put("index.number_of_shards", 1)
					.put("index.number_of_replicas", 1).build()
		).node().start();
    }
    
    @Before public void setUp() throws InterruptedException {
    	client = node.client();
    }
    
    @After public void tearDown() {
    	client.prepareDeleteByQuery(location).setQuery("").setTypes(ElasticSearchConstants.INDEX_TYPE).execute().actionGet();
    	client.admin().indices().prepareDelete(location).execute().actionGet();
    	client.admin().cluster().prepareHealth().setWaitForYellowStatus().setTimeout("10s").execute().actionGet();
    	client = null;
    }
    
    @AfterClass public static void stopCluster() {
    	node.stop();
    }

}
