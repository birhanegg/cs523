/**
 * @project : MIU cs523 project
 * 
 * @Desc: Real Time tweets analysis to get top hashtags using Apache Kafka, Apache Spark, and Hive in Cloudera quickstart VM environment.
 * 
 * @Arguments: <comsumerKey> <consumerSecret> <accessToken> <accessTokenSecret>
 * <comsumerKey>		- Twitter consumer key 
 * <consumerSecret>  	- Twitter consumer secret
 * <accessToken>		- Twitter access token
 * <accessTokenSecret>	- Twitter access token secret
 * 
 * @author Birhane Gebre
 * @Date sep 2021
 * 
 */

package cs523.project;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.Status;

public class SparkStreamProducer {
	
	public static void main(String[] args)throws IOException  {
		Logger.getLogger("org").setLevel(org.apache.log4j.Level.ERROR);

		/* Twitter credentials configuration input from args	*/
		final String consumerKey = args[1];
		final String consumerSecret = args[2];
		final String accessToken = args[3];
		final String accessTokenSecret = args[4];		
		
		/* spark configuration */
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("SparkTwitter-project");
		JavaStreamingContext jssc = new JavaStreamingContext(conf,new Duration(10000));

		System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
		System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
		System.setProperty("twitter4j.oauth.accessToken", accessToken);
		System.setProperty("twitter4j.oauth.accessTokenSecret",accessTokenSecret);
		
		/* kafak configuration */
		Properties properties = new Properties();
		properties.put("bootstrap.servers", "localhost:9092");
		properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
		properties.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");
		KafkaProducer kafkaProducer = new KafkaProducer(properties);

	       
		JavaReceiverInputDStream<Status> enTweets = TwitterUtils.createStream(jssc);

		/* Transformation */
		JavaDStream<String> splitLines = enTweets.flatMap(status -> Arrays.asList(status.getText().split(" ")));
		
		/* Filtering, Transformation  and reduction */
		JavaDStream<String> filteredLines = splitLines.filter(
				word -> word.startsWith("#") && word.length() > 3
						&& word.matches("#[a-zA-Z0-9]*$")).map(	word -> word.trim());
		
		/* Transformation */
		JavaPairDStream<String, Integer> hashTagsPairs = filteredLines
				.mapToPair(hashTag -> new Tuple2<String, Integer>(hashTag, 1));

		/* Transformation */
		JavaPairDStream<Integer, String> topicCounts60 = hashTagsPairs
				.reduceByKeyAndWindow(
						(integer1, integer2) -> (integer1 + integer2),
						Durations.seconds(3600))
				.mapToPair(tuple -> tuple.swap())
				.transformToPair(
						integerStringJavaPairRDD -> integerStringJavaPairRDD
								.sortByKey(false));

		topicCounts60
				.foreachRDD(new Function<JavaPairRDD<Integer, String>, Void>() {
					@Override
					public Void call(
							JavaPairRDD<Integer, String> topicCounts60RDD)
							throws Exception {
						List<Tuple2<Integer, String>> top10Topics = topicCounts60RDD
								.take(10);// get Top 10.

						top10Topics.forEach(tuple -> {
														
							String event = String.format("%s,%d",	tuple._2(), tuple._1());
							System.out.println(String.format("%s, (%d tweets)",	tuple._2(), tuple._1()));
							/* sending to kafka topic */
							kafkaProducer.send(new ProducerRecord("twitter-events", event));
						});
						// Cache somewhere and show UI	
						return null;
					}
				});

		jssc.start();
		// Wait for 60 seconds then exit. To run forever call without a timeout
		jssc.awaitTermination(60000);
		// Stop the streaming context
		jssc.stop();
	}
}