
/////////////////////// Structured Streaming With JSON ////////////////////////////


val streamingDataFrame = spark.readStream.schema(mySchema).csv("file:///vzwhome/riopery/streaming_data/")

val TOPIC_NAME="EagleStreaming"
val IP_address = "10-119-103-6.ebiz.verizon.com:6667"

val HDFS_PATH = "streaming_data/"
streamingDataFrame.selectExpr("CAST(cca_app_num AS STRING) AS key", "to_json(struct(*)) AS value").as[(String, String)].writeStream.format("kafka").option("topic", TOPIC_NAME).option("kafka.bootstrap.servers", IP_address).option("checkpointLocation", HDFS_PATH).start()


///////////////////////////////////////////////////////////////////////////

import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

val mySchema = StructType(Array(
StructField("cca_app_num", StringType), 
StructField("cca_type", StringType),  
StructField("cca_status", StringType),  
StructField("cca_previous", StringType),  
StructField("cca_name", StringType),  
StructField("cca_street_number", StringType),   
StructField("cca_street_name", StringType),  
StructField("cca_street_type", StringType), 
StructField("cca_street_suite", StringType),  
StructField("cca_city", StringType), 
StructField("cca_state", StringType),
StructField("cca_zip", StringType),
StructField("cca_market", StringType), 
StructField("cca_order_type", StringType), 
StructField("cca_phone", StringType),
StructField("cca_current_user", StringType),
StructField("cca_user_name", StringType), 
StructField("cca_timestamp", StringType), 
StructField("cca_num_of_phones", StringType),
StructField("cca_agent_code", StringType),
StructField("cca_region_ind", StringType), 
StructField("cca_bill_city", StringType),
StructField("cca_bill_state", StringType),
StructField("cca_bill_zip", StringType), 
StructField("cca_existing_mobile", StringType),
StructField("cca_conversion_mobile", StringType),
StructField("cca_cust_id_no", StringType),
StructField("cca_street_dir", StringType),
StructField("cca_po_box_num", StringType), 
StructField("cca_rural_rte_num", StringType),
StructField("cca_rural_del_txt", StringType),
StructField("cca_cust_ctry_cd", StringType), 
StructField("cca_bill_rural_del_txt", StringType),
StructField("cca_bill_ctry_cd", StringType), 
StructField("cca_location_code", StringType),
StructField("cca_language_ind", StringType), 
StructField("vzwdb_create_timestamp", StringType), 
StructField("vzwdb_update_timestamp", StringType),
StructField("ing_file_name", StringType), 
StructField("file_commit_timestamp", StringType),
StructField("ing_primary_key", StringType)
))

import spark.implicits._
import java.sql.Timestamp

val df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "10-119-103-6.ebiz.verizon.com:6667").option("subscribe", "EagleStreaming").format("csv")
             .option("delimiter"," ").option("quote","")
             .option("header", "true")
             .schema(customSchema)load()

val df1 = df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)").as[(String, Timestamp)].select(from_json($"value", mySchema).as("data"), $"timestamp").select("data.*", "timestamp")

df1.writeStream.format("console").option("truncate","false").start().awaitTermination()