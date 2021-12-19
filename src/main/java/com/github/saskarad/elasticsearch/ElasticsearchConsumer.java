package com.github.saskarad.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;

public class ElasticsearchConsumer {
    public static RestHighLevelClient createClient(){
        String hostname = "localhost";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic","changeme"));

        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static void main(String[] args) throws IOException {
        Logger logger = LoggerFactory.getLogger(ElasticsearchConsumer.class.getName());
        RestHighLevelClient client = createClient();

        KafkaConsumer<String,String> consumer = Consumer.createConsumer("twitter_tweets");

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(200));
            Integer recordCount = records.count();
            logger.info("Received "  + recordCount + " records");

            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String,String> record : records){
                // 2 strategies to make idempotence
                // - Kafka Generic Id
                // String id = record.topic() + "_" + record.partition() + "_" +record.offset();

                // twitter feed specific id
                try {
                    String id = extractIdFromTweet(record.value());
                    // insert data to elasticsearch
                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id
                    ).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest); // add to bulk request
                }catch (NullPointerException e){
                    logger.warn("Skipping bad data: " + record.value());
                }
            }

            if(recordCount > 0){
                BulkResponse  bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("committing offsets..");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        // close client
        // client.close();
    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String tweetJson){
      return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }

}
