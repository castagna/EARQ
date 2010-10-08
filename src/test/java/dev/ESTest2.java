package dev;

import static org.elasticsearch.client.Requests.countRequest;
import static org.elasticsearch.client.Requests.createIndexRequest;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.refreshRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;

import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.xcontent.WildcardQueryBuilder;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.junit.Assert;
import org.junit.Test;

public class ESTest2 extends Assert {

    private static final String TEST_INDEX = "test_index";
    private static final String TEST_TYPE = "test_type";
    private static final String TEST_ID1 = "1";
    private static final String TEST_USER1 = "paul";
    private static final String USER_FIELD = "user";

    @Test
    public void test() throws IOException {

        Node node = startNode();
        try {
            Client client = node.client();

            createMapping(client);
            index(client);
            client.admin().indices().refresh(refreshRequest()).actionGet();
            count(client);
            get(client);

        } finally {
            node.close();
        }
    }

    private void count(Client client) {
        CountResponse count = client.count(
                countRequest(TEST_INDEX)
                .types(TEST_TYPE).query(new WildcardQueryBuilder(USER_FIELD,
"*"))
        ).actionGet();

        assertNotNull(count);
        assertEquals(1, count.count());

    }

    private Node startNode() {

        Node node =
NodeBuilder.nodeBuilder().loadConfigSettings(false).clusterName("test.cluster").local(true).settings(

                ImmutableSettings.settingsBuilder()
                .put("gateway.type", "none")
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 1).build()

        ).node().start();

        return node;
    }

    private void get(Client client) {

        GetResponse get = client.get(
                getRequest(TEST_INDEX)
                .type(TEST_TYPE)
                .id(TEST_ID1)
                .fields(USER_FIELD)
        ).actionGet();

        assertNotNull(get);

        assertTrue(get.exists());

        assertEquals(TEST_ID1, get.getId());
        assertEquals(TEST_TYPE, get.getType());
        assertEquals(TEST_USER1, get.field(USER_FIELD).values().get(0));

    }

    private void index(Client client) throws IOException {

        XContentBuilder json = jsonBuilder()
            .startObject()
                .field(USER_FIELD, TEST_USER1)
            .endObject();

//        System.out.println(json.string());

        IndexResponse index = client.index(indexRequest(TEST_INDEX)
                .type(TEST_TYPE)
                .id(TEST_ID1)
                .create(true)
                .source(
                        json
                )
        ).actionGet();

        assertNotNull(index);
        assertEquals(TEST_ID1, index.id());
        assertEquals(TEST_TYPE, index.type());
        assertEquals(TEST_INDEX, index.index());
    }

    private void createMapping(Client client) throws IOException {

        XContentBuilder data = jsonBuilder().startObject().startObject("properties")
            .startObject(USER_FIELD)
            .field("type", "string")
            .field("index", "not_analyzed")
            .field("store", "yes")
            .endObject()
        .endObject().endObject();

//        System.out.println(data.string());

        CreateIndexResponse mapping = client.admin().indices().create(
                createIndexRequest(TEST_INDEX).mapping(TEST_TYPE, data)
        ).actionGet();

        assertTrue(mapping.acknowledged());
    }

} 