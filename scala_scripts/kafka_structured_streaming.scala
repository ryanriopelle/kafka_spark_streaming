////////////////////////////////////////////////   Newer Framework  - Structured Streaming   ////////////////////////////////////////////////////////////////////////////////

// Spark context available as 'sc' (master = local[*], app id = local-1531845101317).
// Spark session available as 'spark'.
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import scala.concurrent.duration._
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
//val sc = new SparkContext(conf)


val streamingContext = new StreamingContext(sc, Seconds(60))
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val hiveSqlContext = new HiveContext(sc)
hiveSqlContext.setConf("hive.server2.thrift.port", "10007")
hiveSqlContext.setConf("hive.server2.authentication","NOSASL")
HiveThriftServer2.startWithContext(hiveSqlContext)

val records = spark.readStream.format("kafka").
|option("subscribepattern", "EagleStreaming").
|option("kafka.bootstrap.servers", "10-119-103-6.ebiz.verizon.com:6667").
|option("startingoffsets", "latest").option("maxOffsetsPerTrigger", 1000).
|option("rowsPerSecond", 1000).load

val result = records.select(
$"key" cast "string", $"value" cast "string", $"topic", $"partition",$"offset").
|select((expr("(split(value, ','))[0]").cast("string").as("cca_app_num")), 
|expr("(split(value, ','))[1]").cast("string").as("cca_type"),
|expr("(split(value, ','))[2]").cast("string").as("cca_status"),
|expr("(split(value, ','))[3]").cast("string").as("cca_previous"),
|expr("(split(value, ','))[4]").cast("string").as("cca_name"),
|expr("(split(value, ','))[5]").cast("string").as("cca_street_number"),
|expr("(split(value, ','))[6]").cast("string").as("cca_street_name"),
|expr("(split(value, ','))[7]").cast("string").as("cca_street_type"),
|expr("(split(value, ','))[8]").cast("string").as("cca_street_suite"),
|expr("(split(value, ','))[9]").cast("string").as("cca_city"),
|expr("(split(value, ','))[10]").cast("string").as("cca_state"),
|expr("(split(value, ','))[11]").cast("string").as("cca_zip"),
|expr("(split(value, ','))[12]").cast("string").as("cca_market"),
|expr("(split(value, ','))[13]").cast("string").as("cca_order_type"),
|expr("(split(value, ','))[14]").cast("string").as("cca_phone"),
|expr("(split(value, ','))[15]").cast("string").as("cca_current_user"),
|expr("(split(value, ','))[16]").cast("string").as("cca_user_name"),
|expr("(split(value, ','))[17]").cast("string").as("cca_timestamp"),
|expr("(split(value, ','))[18]").cast("string").as("cca_num_of_phones"),
|expr("(split(value, ','))[19]").cast("string").as("cca_agent_code"),
|expr("(split(value, ','))[20]").cast("string").as("cca_region_ind"),
|expr("(split(value, ','))[21]").cast("string").as("cca_bill_city"),
|expr("(split(value, ','))[22]").cast("string").as("cca_bill_state"),
|expr("(split(value, ','))[23]").cast("string").as("cca_bill_zip"),
|expr("(split(value, ','))[24]").cast("string").as("cca_existing_mobile"),
|expr("(split(value, ','))[25]").cast("string").as("cca_conversion_mobile"),
|expr("(split(value, ','))[26]").cast("string").as("cca_cust_id_no"),
|expr("(split(value, ','))[27]").cast("string").as("cca_street_dir"),
|expr("(split(value, ','))[28]").cast("string").as("cca_po_box_num"),
|expr("(split(value, ','))[29]").cast("string").as("cca_rural_rte_num"),
|expr("(split(value, ','))[30]").cast("string").as("cca_rural_del_txt"),
|expr("(split(value, ','))[31]").cast("string").as("cca_cust_ctry_cd"),
|expr("(split(value, ','))[32]").cast("string").as("cca_bill_rural_del_txt"),
|expr("(split(value, ','))[33]").cast("string").as("cca_bill_ctry_cd"),
|expr("(split(value, ','))[34]").cast("string").as("cca_location_code"),
|expr("(split(value, ','))[35]").cast("string").as("cca_language_ind"),
|expr("(split(value, ','))[36]").cast("string").as("vzwdb_create_timestamp"),
|expr("(split(value, ','))[37]").cast("string").as("vzwdb_update_timestamp"),
|expr("(split(value, ','))[38]").cast("string").as("ing_file_name"),
|expr("(split(value, ','))[39]").cast("string").as("file_commit_timestamp"),
|expr("(split(value, ','))[40]").cast("string").as("ing_primary_key"))


val sq = result.writeStream.format("console").
|option("truncate", false).
|trigger(Trigger.ProcessingTime(10.seconds)).
|outputMode(OutputMode.Append).
|queryName("from-kafka-to-console").start


// val in_memory = result.writeStream.format("console").queryName("query_name").option("truncate", false).
// |trigger(Trigger.ProcessingTime(5.seconds)).outputMode(OutputMode.Append).start


// val parquet = result.writeStream.format("parquet").option("path","/user/riopery/streaming/").
// |option("checkpointLocation","/user/riopery/streaming_checkpoint").option("truncate", false).
// |trigger(Trigger.ProcessingTime(5.seconds)).outputMode("append").start()


streamingContext.start()
streamingContext.awaitTermination()
