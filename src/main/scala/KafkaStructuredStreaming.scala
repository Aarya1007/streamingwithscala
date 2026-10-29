import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object KafkaStructuredStreaming {
  def main(args: Array[String]): Unit = {

    // 1. Create Spark session
    val spark = SparkSession.builder()
      .appName("KafkaStructuredStreaming")
      .master("local[*]") // For local testing only
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

//    spark.sparkContext.hadoopConfiguration.set(
//      "google.cloud.auth.service.account.enable",
//      "true"
//    )
//
//    spark.sparkContext.hadoopConfiguration.set(
//      "google.cloud.auth.service.account.json.keyfile",
//      "D:/keys/project-1b07b18d-b3e4-464f-9f6-86ab63afe640.json"
//    )

    // 2. Read streaming data from Kafka
    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "payment1")
      .option("startingOffsets", "earliest")
      .load()

    // 3. Extract Kafka message value
    val messagesDF = kafkaDF.select(
      col("key").cast("string").as("key"),
      col("value").cast("string").as("message"),
      col("topic"),
      col("partition"),
      col("offset")
    )

    val jsonschema = StructType(List(
      StructField("CreatedTime", StringType),
      StructField("TotalAmount", DoubleType)
      ),
    )

    val jsondf = messagesDF.withColumn("message",from_json(col("message").cast("String"),jsonschema))
    jsondf.printSchema()

    val aggdf = jsondf.selectExpr("message.CreatedTime as CreatedTime",
                                        "message.TotalAmount as TotalAmount")
    val newdf = aggdf.withColumn("CreatedTime",col("CreatedTime").cast("timestamp"))
   // aggdf.printSchema()

    val transformedDF = newdf.withWatermark("CreatedTime","2 minute")
      .groupBy(window(col("CreatedTime"),"5 minute"))
      .agg(sum(col("TotalAmount")).alias("Total"))
    println("---------transformed df--------------------")
    transformedDF.printSchema()

    val outputdf = transformedDF.selectExpr("current_timestamp() as load_time","window.start as start","window.end as end","Total")
    println("---------output df--------------------")
    outputdf.printSchema()

//    val outkafkadf = outputdf.select(col("key"),to_json
//              (struct(col("load_time"),col("start"),col("end"),
//                col("Total"))).alias("value")).select(col("key").cast("String"),col("value").cast("String"))
//    println("---------out kafka df--------------------")
//    outkafkadf.printSchema()
    //outputdf.explain(true)
    // 4. Print incoming messages to console
    val query = outputdf.writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .option("checkpointLocation", "data/payment1/kafka-stream-checkpoint")
      .start()

//    val query = outkafkadf.writeStream
//      .outputMode("complete")
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("topic","payment2")
//      .option("checkpointLocation","data/payment2/kafka-stream-checkpoint")
//      .trigger(Trigger.ProcessingTime("1 minute"))
//      .start()

//    val query = outkafkadf.writeStream
//      .format("parquet")
//      .option("path", "gs://kafka_streaming/")
//      .option("checkpointLocation", "gs://checkpoint_location/payments2/")
//      .outputMode("append")
//      .trigger(Trigger.ProcessingTime("1 minute"))
//      .start()
    // 5. Keep streaming application running
    query.awaitTermination()
  }
}
