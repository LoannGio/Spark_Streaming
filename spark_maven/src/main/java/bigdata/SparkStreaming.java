package bigdata;

import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.conf.ConfigurationContext;

public class SparkStreaming {

	public static void main(String[] args) {
		
		SparkConf sparkConf = new SparkConf()
		        .setAppName("SparkStreaming")
		        .setMaster("local[2]");
		JavaStreamingContext sc = new JavaStreamingContext(sparkConf, new Duration(5000));

		Configuration twitterConf = ConfigurationContext.getInstance();
		Authorization twitterAuth = AuthorizationFactory.getInstance(twitterConf);
		
		String[] filters = { "#Android" };
		TwitterUtils.createStream(sc, twitterAuth, filters)
<<<<<<< HEAD
		        .flatMap(s -> Arrays.asList(s.getHashtagEntities()))
		        .map(h -> h.getText().toLowerCase())
		        .filter(h -> !h.equals("android"))
		        .countByValue()
		        .print();
		
=======
			.flatMap(s -> Arrays.stream(s.getHashtagEntities()).iterator())
			.map(h -> h.getText().toLowerCase())
			.filter(h -> !h.equals("android"))
			.countByValue()
			.print();

>>>>>>> 59b6f913e7983fccbaba58fd43da4bca735a97d0
		sc.start();
		sc.awaitTermination();
	}
	
}
