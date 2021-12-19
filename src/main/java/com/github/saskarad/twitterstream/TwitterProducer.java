package com.github.saskarad.twitterstream;

import com.google.common.collect.Lists;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    public TwitterProducer(){}
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());


    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run(){
        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // create twitter client
        List<String> terms = Lists.newArrayList("engineering","software engineer", "kafka");
        Client client = TwitterClient.createTwitterClient(msgQueue,terms);
        client.connect();

        // create kafka producer
        KafkaProducer<String,String> producer = Producer.createProducer();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping app....");
            logger.info("Shutting down client from twitter...");
            client.stop();
            logger.info("closing producer...");
            producer.close();
        }));

        // loop to send tweets
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if (e != null){
                            logger.error("Something bad happened", e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }

}
