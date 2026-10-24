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
    //jsondf.printSchema()

    val aggdf = jsondf.selectExpr("key","message.CreatedTime as CreatedTime",
                                        "message.TotalAmount as TotalAmount")
    val newdf = aggdf.withColumn("CreatedTime",col("CreatedTime").cast("timestamp"))
   // aggdf.printSchema()

    val transformedDF = newdf.withWatermark("CreatedTime","2 minute")
      .groupBy(window(col("CreatedTime"),"5 minute"))
      .agg(sum(col("TotalAmount")).alias("Total"))


   // transformedDF.printSchema()

    val outputdf = transformedDF.selectExpr("current_timestamp() as load_time","window.start as start","window.end as end","Total")
    outputdf.printSchema()
    // 4. Print incoming messages to console
    val query = outputdf.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .option("checkpointLocation", "data/payment1/kafka-stream-checkpoint")
      .start()

    // 5. Keep streaming application running
    query.awaitTermination()
  }
}
