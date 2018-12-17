package bigdata;

import java.util.Arrays;

import org.apache.spark.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.TwitterUtils;
import twitter4j.auth.Authorization;
import twitter4j.auth.AuthorizationFactory;
import twitter4j.conf.Configuration;
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
			.flatMap(s -> Arrays.stream(s.getHashtagEntities()).iterator())
			.map(h -> h.getText().toLowerCase())
			.filter(h -> !h.equals("android"))
			.countByValue()
			.print();

		sc.start();
		sc.awaitTermination();
	}
	
}
