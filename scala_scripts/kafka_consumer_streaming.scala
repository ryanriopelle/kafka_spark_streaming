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

val streamingContext = new StreamingContext(sc, Seconds(10))
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val hiveSqlContext = new HiveContext(sc)

hiveSqlContext.setConf("hive.server2.thrift.port", "10007")
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
val topics = Array("EagleStreaming_1")

//Raw stream Info
val stream = KafkaUtils.createDirectStream[String, String](
streamingContext,
PreferConsistent,
Subscribe[String, String](topics, kafkaParams))

case class employee(eid: String, name: String, salary: String, destination: String)

// var inputStream = stream.map(record=>(record.value().toString))
var inputStream = stream.map (record=> record.value().toString)

inputStream.foreachRDD(rdd => if (!rdd.isEmpty()) {
  val streamDF = rdd.map { record => {
              val recordArr = record.split(",")
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
spark.sql("SELECT * FROM streamDF").show()
// var dataDf = sqlContext.sql("SELECT message.data.* FROM streamDF")
// dataDf.printSchema()
//dataDf.collect
// dataDf.createOrReplaceTempView("streamingTable")
})


HiveThriftServer2.startWithContext(hiveSqlContext)
streamingContext.start()


// val record_array_list = """recordArr(0),recordArr(1),recordArr(2),
// |recordArr(3),recordArr(4),recordArr(5),recordArr(6),recordArr(7),
// |recordArr(8),recordArr(9),recordArr(10),recordArr(11),recordArr(12),
// |recordArr(13),recordArr(14),recordArr(15),recordArr(16),recordArr(17),
// |recordArr(18),recordArr(19),recordArr(20),recordArr(21),recordArr(22),
// |recordArr(23),recordArr(24),recordArr(25),recordArr(26),recordArr(27),
// |recordArr(28),recordArr(29),recordArr(30),recordArr(31),recordArr(32),
// |recordArr(33),recordArr(34),recordArr(35),recordArr(36),recordArr(37),
// |recordArr(38),recordArr(39),recordArr(40)"""

// val small_record_array = """
// recordArr(0),recordArr(1),recordArr(2),recordArr(3),recordArr(4),
// recordArr(5),recordArr(6),recordArr(7),recordArr(8),recordArr(9),
// recordArr(10),recordArr(11),recordArr(12),recordArr(13),recordArr(14),
// recordArr(15),recordArr(16),recordArr(17),recordArr(18),recordArr(19),
// recordArr(20),recordArr(21)
// 	"""

// val schema_list = """"cca_app_num","cca_type","cca_status","cca_previous","cca_name","cca_street_number",
// |"cca_street_name","cca_street_type","cca_street_suite","cca_city","cca_state",
// |"cca_zip","cca_market","cca_order_type","cca_phone","cca_current_user","cca_user_name",
// |"cca_timestamp","cca_num_of_phones","cca_agent_code","cca_region_ind","cca_bill_city",
// |"cca_bill_state","cca_bill_zip","cca_existing_mobile","cca_conversion_mobile","cca_cust_id_no",
// |"cca_street_dir","cca_po_box_num","cca_rural_rte_num","cca_rural_del_txt","cca_cust_ctry_cd",
// |"cca_bill_rural_del_txt","cca_bill_ctry_cd","cca_location_code","cca_language_ind",
// |"vzwdb_create_timestamp","vzwdb_update_timestamp","ing_file_name",
// |"file_commit_timestamp","ing_primary_key""""

// val small_schema_list = """
// "cca_app_num","cca_type","cca_status","cca_previous","cca_name",
// "cca_street_number","cca_street_name","cca_street_type","cca_street_suite","cca_city",
// "cca_state","cca_zip","cca_market","cca_order_type","cca_phone",
// "cca_current_user","cca_user_name","cca_timestamp","cca_num_of_phones","cca_agent_code",
// "cca_region_ind","cca_bill_city"
// """


// val schema_list = """"cca_app_num","cca_type","cca_status","cca_previous","cca_name","cca_street_number",
// |"cca_street_name","cca_street_type","cca_street_suite","cca_city","cca_state",
// |"cca_zip","cca_market","cca_order_type","cca_phone","cca_current_user","cca_user_name",
// |"cca_timestamp","cca_num_of_phones","cca_agent_code","cca_region_ind","cca_bill_city",
// |"cca_bill_state","cca_bill_zip","cca_existing_mobile","cca_conversion_mobile","cca_cust_id_no",
// |"cca_street_dir","cca_po_box_num","cca_rural_rte_num","cca_rural_del_txt","cca_cust_ctry_cd",
// |"cca_bill_rural_del_txt","cca_bill_ctry_cd","cca_location_code","cca_language_ind",
// |"vzwdb_create_timestamp","vzwdb_update_timestamp","ing_file_name",
// |"file_commit_timestamp","ing_primary_key"""


// val file = sc.textFile("rdd_of_data-1532019240000").toJavaRDD


