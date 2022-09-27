package cs523.project;

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
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private Logger logger= LoggerFactory.getLogger(TwitterProducer.class);

    private String consumerKey="";
    private String consumerSecret="";
    private String token="";
    private String secret="";
    private String TOPIC="tweet";
    private List<String> keyWords = Lists.newArrayList("America");//keyword to filter tweets 


    public TwitterProducer(){}
    public static void main(String[] args) {
       new TwitterProducer().run();
    }

    public void run(){
        logger.info("Setup");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(30);

        /* *********1. create a twitter client    **********/       
        Client client=createTwitterClient(msgQueue);
        client.connect();
        /* ***********2. create a kafka producer    **********/

        KafkaProducer<String, String> producer=createKafkaProducer();

        //shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("stopping application...");
            logger.info("shutting down client from twitter . . .");
            client.stop();
            logger.info("closing producer...");
            producer.close();
            logger.info("Done!");
        }));

        /* ***********3. loop to send tweets to kafka **********/
        // on a different thread or multiple different threads,

        while(!client.isDone()){
            String msg=null;
            try {
                msg=msgQueue.poll(5, TimeUnit.SECONDS);

            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if(msg!=null){
                logger.info(msg);
                System.out.println(msg);
                producer.send(new ProducerRecord<>(TOPIC, null, msg), new Callback() {//tweet
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("Something bad happened",e);
                        }
                    }
                });
            }
        }
        logger.info("End of application");
    }




    public Client createTwitterClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(keyWords);

        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        
        Client hosebirdClient = builder.build();
        return hosebirdClient;

    }


    public KafkaProducer<String, String> createKafkaProducer() {
        /***************1. create producer properties******************/
       
        Properties properties=new Properties();
        
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());        
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024)); 


        /*****************2. create the producer*************************/
      

        return new KafkaProducer<String, String>(properties);
    }
}
