package cs523.project;

import java.io.IOException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import kafka.serializer.StringDecoder;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.slf4j.LoggerFactory;

import scala.Tuple2;


public class SparkKafkaConsumer {

    private  static long startTime = 0;
	private static final String TABLE_NAME = "twitter";
	private static final String TWEETS = "Tweets";
	private static long duration=0;
	private static final String HASHTAG="HashTag";
	private static final String OCCURRENCES="Occurrences";
	private static org.slf4j.Logger logger= LoggerFactory.getLogger(SparkKafkaConsumer.class);


	public static void main(String[] args) throws IOException {

		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		
		/***************1. Hbase configuration******************/
		Configuration config = HBaseConfiguration.create();
		config.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);

		try (Connection connection = ConnectionFactory.createConnection(config);
				Admin admin = connection.getAdmin()) {
			HColumnDescriptor columnDescriptor = new HColumnDescriptor(TWEETS);
			HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
			table.addFamily(columnDescriptor.setCompressionType(Algorithm.NONE));
			System.out.println("Creating table.... ");
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());				
			}
			admin.createTable(table);
			System.out.println("Table created !");
		}
		
		/***************2. Spark configuration ******************/
		System.out.println("Spark Streaming starting .....");
		SparkConf conf = new SparkConf().setAppName("TwitterStreamApp").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);		
		// batch time interval
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));	
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		Set<String> topics = Collections.singleton("tweet");
            
        try {  		      	
        	
        	/***************3. Spark consuming twitter streaming tweets, filter hashtags and persist  to hbase in realtime ******************/
            sparkStreamAndHbaseIntegration(sc, ssc, kafkaParams, topics,config);     
            
            ssc.start();
            ssc.awaitTermination();

        } catch (Exception e) {
        	logger.error("Spark Context Stopped... "+e.getLocalizedMessage());
        } finally {
        	
        	
        	/***************4. Read TopTen Trending Hashtags from Hbase ******************/
        	topTenTrending(sc,config);             
        }
	}
	
    @SuppressWarnings({ "deprecation", "resource" })
	private static void sparkStreamAndHbaseIntegration(JavaSparkContext sc, JavaStreamingContext ssc, Map<String, String> kafkaParams, Set<String> topics, Configuration config) throws IOException {
		HTable hTable = new HTable(config, TABLE_NAME);
		startTime=Calendar.getInstance().getTimeInMillis();
		System.out.println("Streaming start time: " + startTime);
        JavaPairInputDStream<String, String> streamData = 
        		KafkaUtils.createDirectStream(ssc, String.class,String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        AtomicInteger i = new AtomicInteger(0);    
        
        streamData.foreachRDD(rdd -> {
			if (rdd.count() == 0 && i.get() > 2) {
				  duration = (Calendar.getInstance().getTimeInMillis() - startTime) / 1000;
				  System.out.println("Done ..............");
				throw new Exception("Done Waiting, shutting down ...");
			}
			i.getAndIncrement();
			System.out.println("New Batch of data arrived  "+ rdd.partitions().size() + " Partitions and "+ rdd.count() + " Records");

			if (rdd.count() > 0) {
				System.out.println("Inserting a batch of streaming data to Hbase in real-time...");
				rdd.flatMap(rd -> {
									String word = StringEscapeUtils.unescapeJava(rd._2);
									word = word.replaceAll("[^a-zA-Z0-9#]", " ");
									return Arrays.asList(word.split(" "));
								}).filter(rd -> rd.contains("#")).filter(rd -> rd.length() > 1).mapToPair(hashtag -> {
									if (hashtag.indexOf("#") != 0) {
										List<String> words = Arrays.asList(hashtag.split("#"));
										if (words.size() > 1) {
					                        hashtag = "#" + words.get(1);
					                    }
									}
									return new Tuple2<>(hashtag, 1);
								}).reduceByKey(Integer::sum).mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap).collect().forEach(r -> {
									Integer row = i.getAndIncrement();
									Put p = new Put(Bytes.toBytes(row.toString()));
									p.add(Bytes.toBytes(TWEETS),Bytes.toBytes(HASHTAG),Bytes.toBytes(r._1.toString()));
									p.add(Bytes.toBytes(TWEETS),Bytes.toBytes(OCCURRENCES),Bytes.toBytes(r._2.toString()));

									try {
										hTable.put(p);
									} catch (Exception e) {
										logger.error("Error inserting row!");
										e.printStackTrace();
									}			
//										System.out.println("[" + r._1.toString()+ ":" + r._2 + "]"); //demo purpose 
								});
			}
		});
   }   

	private static void topTenTrending(JavaSparkContext sc, Configuration config) {
		JavaPairRDD<ImmutableBytesWritable, Result> javaPairRdd = 
				sc.newAPIHadoopRDD(config, TableInputFormat.class,ImmutableBytesWritable.class, Result.class);
		System.out.println("Top 10 Trending Hashtags in the last: " + duration / 60 + " Minutes, "+" of total records: "+javaPairRdd.count());
		String leftAlignFormat = "| %-25s | %-12d |%n";
		System.out.format("+---------------------------+--------------+%n");
		System.out.format("| HashTag                   | Occurrences  |%n");
		System.out.format("+---------------------------+--------------+%n");
		if (Objects.nonNull(javaPairRdd) && javaPairRdd.count() > 0) {		
			javaPairRdd.mapToPair(rdd -> {
								byte[] hashTagByte = rdd._2().getValue(Bytes.toBytes(TWEETS),Bytes.toBytes(HASHTAG));
								byte[] occurrenceByte = rdd._2().getValue(Bytes.toBytes(TWEETS),Bytes.toBytes(OCCURRENCES));

								String hashTag = Bytes.toString(hashTagByte);
								Integer occur = Integer.parseInt(Bytes.toString(occurrenceByte));

								return new Tuple2<>(hashTag, occur);
							}).reduceByKey(Integer::sum).mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap).take(10)
							.forEach(t -> System.out.format(leftAlignFormat, t._1, t._2 ));
		System.out.format("+---------------------------+--------------+%n");
			
			sc.stop();
		}	
	}
}
