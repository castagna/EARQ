package dev;

import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.admin.cluster.state.ClusterStateRequestBuilder;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class ElasticSearchServer {

	public static void main(String[] args) throws InterruptedException {
    	Node server = NodeBuilder.nodeBuilder().loadConfigSettings(false).clusterName("test.cluster").local(true).settings(
				ImmutableSettings.settingsBuilder()
					.put("network.host", "127.0.0.1")
					.put("index.store.type", "memory")
					.put("gateway.type", "none")
					.put("index.number_of_shards", 1)
					.put("index.number_of_replicas", 1).build()
		).node().start();
    	
    	Node node = NodeBuilder.nodeBuilder().client(true).loadConfigSettings(false).clusterName("test.cluster").local(true).settings(
				ImmutableSettings.settingsBuilder()
					.put("network.host", "127.0.0.1")
					.put("index.store.type", "memory")
					.put("gateway.type", "none")
					.put("index.number_of_shards", 1)
					.put("index.number_of_replicas", 1).build()
		).node().start();
    	
    	Client client = node.client();
		ClusterStateRequestBuilder request = client.admin().cluster().prepareState();
		ClusterStateResponse response = request.execute().actionGet();
		System.out.println(response.getClusterName());

		server.stop();
		node.stop();
	}

}
