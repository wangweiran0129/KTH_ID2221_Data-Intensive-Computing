package sparkstreaming

import java.util.HashMap

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object KafkaSpark {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = {'class': 'SimpleStrategy','replication_factor':1};")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")
    // make a connection to Kafka and read (key, value) pairs from it
    val kafkaConf = new SparkConf().setMaster("local[4]").setAppName("SparkPipeline")
    val ssc = new StreamingContext(kafkaConf, Seconds(2))
    val topics = "avg"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "0",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    ssc.checkpoint(".")

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    val pairs = messages.map(x=>(x.key,x.value.toDouble))
    //pairs.print()
    
    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
        val (sum, cnt) = state.getOption.getOrElse((0.0, 0))
        val newSum = value.getOrElse(0.0) + sum
        val newCnt = cnt + 1
        state.update((newSum, newCnt))
        (key, newSum/newCnt)

    }

    val stateAvgCount = pairs.mapWithState(StateSpec.function(mappingFunc _))
    //stateAvgCount.print()


    // store the result in Cassandra
    stateAvgCount.saveToCassandra("avg_space","avg", SomeColumns("word","count"))
    ssc.start()
    ssc.awaitTermination()

  }
}
