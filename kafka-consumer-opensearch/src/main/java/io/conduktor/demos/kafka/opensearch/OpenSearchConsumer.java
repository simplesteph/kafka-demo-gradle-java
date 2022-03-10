package io.conduktor.demos.kafka.opensearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://he6de7ka5o:yozz8ryqmg@kafka-course-5842482143.eu-west-1.bonsaisearch.net:443";


        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-opensearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }

    private static String extractId(String json) {
        // gson library
        return JsonParser.parseString(json)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }


    public static void main(String[] args) throws IOException {

        String indexName = "wikimedia";

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
        RestHighLevelClient client = createOpenSearchClient();
        KafkaConsumer<String, String> consumer = createConsumer("wikimedia.recentchange");


        try (client; consumer) {

            // create the index in case it doesn't already exist

            if (!client.indices().exists(new GetIndexRequest(indexName), RequestOptions.DEFAULT)) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Wikimedia index created");
            } else {
                log.info("Wikimedia index already created");
            }

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " records");

                BulkRequest bulkRequest = new BulkRequest();

                for (ConsumerRecord<String, String> record : records) {

                    // 2 strategies
                    // kafka generic ID
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    try {
                        String id = extractId(record.value());

                        // where we insert data into ElasticSearch

                        IndexRequest indexRequest = new IndexRequest(indexName)
                                .source(record.value(), XContentType.JSON)
                                .id(id); // this is to make our consumer idempotent

                        bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
                    } catch (NullPointerException e) {
                        log.warn("skipping bad data: " + record.value());
                    }

                }

                if (bulkRequest.numberOfActions() > 0) {
                    BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("Inserted " + bulkItemResponses.getItems().length + " record(s).");

                    // add a bit of delay to make the bulk requests more efficient
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();

                    }

                }

            }
        }
    }
}
