package com.verizon

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


object EagleStreaming{ //if spark shell comment out
  def main(args: Array[String]){ //if spark shell comment out

	val conf = new SparkConf()
	conf.setAppName("EagleStreaming")
	val sc = new SparkContext(conf)  //if spark shell comment out
	val streamingContext = new StreamingContext(sc, Seconds(60))
	val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	val hiveSqlContext = new HiveContext(sc)
	hiveSqlContext.setConf("hive.server2.thrift.port", "10006")
	hiveSqlContext.setConf("hive.server2.authentication","NOSASL")

	// "zookeeper.connect" -> "tbldakf01adv-hdp.tdc.vzwcorp.com:6667",
	//"10-119-103-6.ebiz.verizon.com:6667"
	//10-119-100-141.ebiz.verizon.com:2181  returns bootstrap broker discounnected
	//"tbldakf01adv-hdp.tdc.vzwcorp.com:6667","tbldakf02adv-hdp.tdc.vzwcorp.com:6667"
	val kafkaParams = Map[String, Object](
	"bootstrap.servers" -> "10-119-103-6.ebiz.verizon.com:6667",
	"group.id" -> "${UUID.randomUUID().toString}",
	"key.deserializer" -> classOf[StringDeserializer],
	"value.deserializer" -> classOf[StringDeserializer],
	"auto.offset.reset" -> "earliest",
	"enable.auto.commit" -> (false: java.lang.Boolean))

	//Can list mutliple topics here if wanted
	// EagleStreaming_2
	// cdl_dev_eagle_poc 
	val topics = Array("EagleStreaming_24")

	//Raw stream Info
	val stream = KafkaUtils.createDirectStream[String, String](
	streamingContext,
	PreferConsistent,
	Subscribe[String, String](topics, kafkaParams))

	//case class employee(eid: String, name: String, salary: String, destination: String)

	// var inputStream = stream.map(record=>(record.value().toString))
	var inputStream = stream.map (record=> record.value().toString)

	inputStream.foreachRDD(rdd => if (!rdd.isEmpty()) {
	  val streamDF = rdd.map { record => {
	              val recordArr = record.split("|")
	              (recordArr(0),recordArr(1),recordArr(2),recordArr(3),recordArr(4),
	recordArr(5),recordArr(6),recordArr(7),recordArr(8),recordArr(9),
	recordArr(10),recordArr(11),recordArr(12),recordArr(13),recordArr(14),
	recordArr(15),recordArr(16),recordArr(17),recordArr(18),recordArr(19),
	recordArr(20),recordArr(21))
	            } }.toDF("cca_app_num","cca_type","cca_status","cca_previous","cca_name",
	"cca_street_number","cca_street_name","cca_street_type","cca_street_suite","cca_city",
	"cca_state","cca_zip","cca_market","cca_order_type","cca_phone",
	"cca_current_user","cca_user_name","cca_timestamp","cca_num_of_phones","cca_agent_code",
	"cca_region_ind","cca_bill_city")

	streamDF.createOrReplaceTempView("streamDF")
	// spark.sql("DROP TABLE IF EXISTS eagle.eagle_streaming_poc_1")
	sqlContext.sql("set hive.exec.dynamic.partition=true")
	sqlContext.sql("SET hive.exec.dynamic.partition.mode = nonstrict")
	sqlContext.sql("CREATE TABLE IF NOT EXISTS eagle.eagle_streaming_poc_1 (cca_type String, cca_status String, cca_previous String, cca_name String, cca_street_number String, cca_street_name String, cca_street_type String, cca_street_suite String, cca_city String, cca_state String, cca_zip String, cca_market String, cca_order_type String, cca_phone String, cca_current_user String, cca_user_name String, cca_timestamp String, cca_num_of_phones String, cca_agent_code String, cca_region_ind String, cca_bill_city String) PARTITIONED BY (cca_app_num STRING)")
	sqlContext.sql("INSERT OVERWRITE TABLE eagle.eagle_streaming_poc_1 PARTITION (cca_app_num) SELECT * FROM streamDF")
	sqlContext.sql("SELECT * FROM eagle.eagle_streaming_poc").show
	})

	HiveThriftServer2.startWithContext(hiveSqlContext)
	streamingContext.start()
  }
}
