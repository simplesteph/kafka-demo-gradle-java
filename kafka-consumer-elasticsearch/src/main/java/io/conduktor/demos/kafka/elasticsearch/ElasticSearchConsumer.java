package io.conduktor.demos.kafka.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {

    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class.getSimpleName());

    public static RestClient createRestClient() {

        // replace with your own credentials if you use BONSAI for example

        String hostname = ""; // localhost or bonsai url
        Integer port = 9200; // localhost or bonsai url
        String username = ""; // needed only for bonsai
        String password = ""; // needed only for bonsai

        if (username.equals("")) {
            // Create the low-level client
            RestClientBuilder builder = RestClient.builder(
                    new HttpHost("localhost", 9200));
            return builder.build();
        } else {
            final CredentialsProvider credentialsProvider =
                    new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(username, password));

            RestClientBuilder builder = RestClient.builder(
                            new HttpHost(hostname, port))
                    .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(
                                HttpAsyncClientBuilder httpClientBuilder) {
                            return httpClientBuilder
                                    .setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });

            return builder.build();

        }
    }

    public static ElasticsearchClient createESClient() {

        RestClient restClient = createRestClient();

        // Create the transport with a Jackson mapper
        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new JacksonJsonpMapper());

        // And create the API client
        return new ElasticsearchClient(transport);

    }

    public static KafkaConsumer<String, String> createConsumer(String topic) {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));

        return consumer;

    }


    public static void main(String[] args) throws IOException, InterruptedException {
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        ElasticsearchClient restClient = createESClient();

        KafkaConsumer<String, String> consumer = createConsumer("wikimedia.recentchange");

        // create the wikimedia index
        String indexname = "wikimedia";
        if (!restClient.indices().exists(c -> c.index(indexname)).value()) {
            restClient.indices().create(c -> c.index(indexname));
        }

        ArrayList<BulkOperation> bulkOperationList = new ArrayList<>();
        ObjectMapper mapper = new ObjectMapper();


        // get a reference to the current thread
        final Thread mainThread = Thread.currentThread();

        // adding the shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected a shutdown, let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {

            while (true) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                int recordCount = records.count();
                logger.info("Received " + recordCount + " records");

                for (ConsumerRecord<String, String> record : records) {

                    try {
                        JsonNode actualObj = mapper.readTree(record.value());
                        String id = actualObj.get("meta").get("id").asText();

                        BulkOperation bulkOperation = new BulkOperation.Builder()
                                .index(c -> c.index(indexname).id(id).document(actualObj)).build();

                        bulkOperationList.add(bulkOperation); // we add to our bulk request (takes no time)

                    } catch (NullPointerException e) {
                        logger.warn("skipping bad data: " + record.value());
                    }

                }

                if (recordCount > 0) {
                    BulkRequest bulkRequest = new BulkRequest.Builder()
                            .index(indexname)
                            .operations(bulkOperationList)
                            .build();

                    BulkResponse response = restClient.bulk(bulkRequest);
                    logger.info("Committing offsets...");
                    consumer.commitSync();
                    logger.info("Offsets have been committed");
                    bulkOperationList.clear();
                    if (recordCount < 100) {
                        Thread.sleep(1000);
                    }
                }
            }
        } catch (WakeupException e) {
            log.info("Wake up exception!");
            // we ignore this as this is an expected exception when closing a consumer
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close(); // this will also commit the offsets if need be.
            log.info("The consumer is now gracefully closed.");
            // close the rest client gracefully
            restClient._transport().close();
            log.info("The ElasticSearch REST Client is now gracefully closed.");
        }

    }
}
