/////////////////////////////////////// Older Framework  - Kafka Consumer Method ///////////////////////////


import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage._
import org.apache.spark.streaming.{StreamingContext, Seconds, Minutes, Time}
import org.apache.spark.streaming.dstream._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.thriftserver.HiveThriftServer2
import org.apache.spark.sql.hive.thriftserver._
import org.apache.kafka.clients.consumer.ConsumerRecord 
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe


// object KafkaStreamingPOC{
//   def main(args: Array[String]){
val conf = new SparkConf()
conf.setAppName("KafkaStreamingPOC")

//val sc = new SparkContext(conf)

val streamingContext = new StreamingContext(sc, Seconds(60))
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val hiveSqlContext = new HiveContext(sc)

hiveSqlContext.setConf("hive.server2.thrift.port", "10007")
hiveSqlContext.setConf("hive.server2.authentication","NOSASL")

//10-119-103-6.ebiz.verizon.com:6667  //
//10-119-100-141.ebiz.verizon.com:2181  returns bootstrap broker discounnected
val kafkaParams = Map[String, Object](
"bootstrap.servers" -> "10-119-103-6.ebiz.verizon.com:6667",
"group.id" -> "${UUID.randomUUID().toString}",
"key.deserializer" -> classOf[StringDeserializer],
"value.deserializer" -> classOf[StringDeserializer],
"auto.offset.reset" -> "earliest",
"enable.auto.commit" -> (false: java.lang.Boolean))

//Can list mutliple topics here if wanted
val topics = Array("EagleStreaming_2")

//Raw stream Info
val stream = KafkaUtils.createDirectStream[String, String](
streamingContext,
PreferConsistent,
Subscribe[String, String](topics, kafkaParams))

var inputStream = stream.map(record=>(record.value().toString))

inputStream.foreachRDD(rdd => if (!rdd.isEmpty()) {
  rdd.collect().foreach(println)
})

HiveThriftServer2.startWithContext(hiveSqlContext)
streamingContext.start()

//When does this actually terminate can kill from the job tracker
streamingContext.awaitTermination()
}
}



