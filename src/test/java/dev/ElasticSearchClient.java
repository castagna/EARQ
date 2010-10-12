package dev;

import org.elasticsearch.action.admin.cluster.ping.single.SinglePingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.action.admin.cluster.ping.single.SinglePingRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public class ElasticSearchClient {

	public static void main(String[] args) {
		Client client = new TransportClient().addTransportAddress(new InetSocketTransportAddress("127.0.0.1", 9200));
		SinglePingRequestBuilder request = client.admin().cluster().preparePingSingle();
		SinglePingResponse response = request.execute().actionGet();
		System.out.println(response);
	}

}
