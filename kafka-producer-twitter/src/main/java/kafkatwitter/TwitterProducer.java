package kafkatwitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private Secrets secrets = new Secrets();
    private String consumerKey = secrets.getConsumerKey();
    private String consumerSecret = secrets.getConsumerSecret();
    private String token = secrets.getToken();
    private String secret = secrets.getSecret();

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private List<String> terms = Lists.newArrayList("got", "game of thrones",
            "bran", "tyrion");

    private TwitterProducer() {}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private void run() {
        logger.info("Setup");

        /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);

        // Create twitter client
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // Create kafka producer
        Producer producer = new Producer();

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping application");
            logger.info("Stopping Twitter client...");
            client.stop();
            logger.info("Closing Kafka producer...");
            producer.end();
            logger.info("Done!");
        }));

        // loop to send tweets
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg != null) {
                logger.info(msg);
                producer.setRecord("twitter_tweets", null, msg);
                producer.sendRecord();
            }
        }
        logger.info("End of application");
    }


    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret,
                token, secret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")     // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }
}
