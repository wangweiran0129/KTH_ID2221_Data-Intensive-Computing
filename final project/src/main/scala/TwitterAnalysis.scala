package org.srp.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.twitter._

import java.util.Properties
import scala.collection.JavaConverters._

import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import edu.stanford.nlp.util.CoreMap

import edu.stanford.nlp.parser.lexparser.LexicalizedParser
import edu.stanford.nlp.parser.lexparser.Options

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._
//API key 
//FsUQDFxbSZux3N68kzuZYWdeP
//API secret key
//41yTorcoIDjScLhgZEpHP3RexSu9iVXxf46slDTTZAiSStsXLn
//Bearer token 
//AAAAAAAAAAAAAAAAAAAAAMzHIwEAAAAAdDtgpJtD9iYTXYdRTqCZ2DF7aYU%3DJ2cnuLUw2uGm21aYRT13O0yZl5I2bt5IsMPqLKLnYyeibBgK9I
//Access token 
//1316476874357440514-il8xbq5U4FClkOVyL6ReqMqKQvVvzf
//Access token secret
//zd9pdvsCnst923cguzBKEY14zcn4vEZQS51ta9oWcSjCS

object TwitterAnalysis{
	def main(args: Array[String]): Unit = {
		System.setProperty("twitter4j.oauth.consumerKey","FsUQDFxbSZux3N68kzuZYWdeP")
		System.setProperty("twitter4j.oauth.consumerSecret", "41yTorcoIDjScLhgZEpHP3RexSu9iVXxf46slDTTZAiSStsXLn")
		System.setProperty("twitter4j.oauth.accessToken", "1316476874357440514-il8xbq5U4FClkOVyL6ReqMqKQvVvzf")
		System.setProperty("twitter4j.oauth.accessTokenSecret", "zd9pdvsCnst923cguzBKEY14zcn4vEZQS51ta9oWcSjCS")

	    val sparkConf = new SparkConf().setMaster("local[4]").setAppName("TwitterSpark")
	    val ssc = new StreamingContext(sparkConf, Seconds(1))
	    ssc.sparkContext.setLogLevel("OFF");
	    
	    val filters = Array("COVID19", "covid19", "coronavirus", "covid-19", "COVID-19", "pandemic")
	    val tweets = TwitterUtils.createStream(ssc, None, filters)
	    //val tweets = TwitterUtils.createStream(ssc, None)
	    val tweetsPlace = tweets.filter(x => x.getPlace != null)
	    val english = tweetsPlace.filter(x => x.getLang() == "en")

	    val keyvalue = english.map(x => (x.getPlace.getCountry, SentimentAnalysis(x.getText()).toInt))
	   	val keyTweet = english.map(x => (x.getPlace.getCountry, x.getText()))

	    keyvalue.print()
	    keyTweet.print()

	    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
	    val session = cluster.connect()

	    session.execute("CREATE KEYSPACE IF NOT EXISTS sentiment_keyspace WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1};")
	    session.execute("CREATE TABLE IF NOT EXISTS sentiment_keyspace.sentiment_count (country text PRIMARY KEY, sentiment text, count int);")

        ssc.checkpoint(".")


	    def mappingFunc(key: String, value: Option[Int], state: State[(Int, Int, Int, Int, Int)]): (String, String, Int) = {
        	val (vneg, neg, neu, pos, vpos) = state.getOption.getOrElse((0, 0, 0, 0, 0))
        	val sent = value.getOrElse(5)
			val SentimentText = Array("Very Negative", "Negative", "Neural", "Positive", "Very Positive", "None")

        	if (sent == 0){
        		val newCnt = vneg + 1
		        state.update((newCnt, neg, neu, pos, vpos))
		        (key + sent.toString, SentimentText(sent), newCnt)
        	} else if (sent == 1){
        		val newCnt = neg + 1
		        state.update((vneg, newCnt, neu, pos, vpos))
		        (key + sent.toString, SentimentText(sent), newCnt)
        	} else if (sent == 2){
        		val newCnt = neu + 1
		        state.update((vneg, neg, newCnt, pos, vpos))
		        (key + sent.toString, SentimentText(sent), newCnt)
		    } else if (sent == 3){
        		val newCnt = pos + 1
		        state.update((vneg, neg, neu, newCnt, vpos))
		        (key + sent.toString, SentimentText(sent), newCnt)
		    } else if (sent == 4){
        		val newCnt = vpos + 1
		        state.update((vneg, neg, neu, pos, newCnt))
		        (key + sent.toString, SentimentText(sent), newCnt)
		    } else {
		    	(key + sent.toString, SentimentText(sent), 0)
		    }
		}

		// Store the data in Cassandra(
	    val stateDstream = keyvalue.mapWithState(StateSpec.function(mappingFunc _))
	    stateDstream.saveToCassandra("sentiment_keyspace", "sentiment_count", SomeColumns("country", "sentiment", "count"))

	    ssc.start()
	    ssc.awaitTermination()

	}

	def SentimentAnalysis(tweets: String): Int = {
		val props = new Properties()
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
		val sentimentPipeline = new StanfordCoreNLP(props)
		val annotation = sentimentPipeline.process(tweets)
		val analysis = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
			.asScala
			.head 
			.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree]) 
		RNNCoreAnnotations.getPredictedClass(analysis)
	}
}