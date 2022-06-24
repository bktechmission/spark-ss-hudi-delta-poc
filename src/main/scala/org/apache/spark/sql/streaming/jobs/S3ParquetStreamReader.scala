package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.utils.Config
import org.apache.spark.sql.types._


object S3ParquetStreamReader extends Logging {
  def main(args: Array[String]) {
    // We have to always pass the first argument as either cloud or local. local is Macbook
    if (args.length == 0) {
      log.error("Environment is required. (Cloud or Local)")
      System.exit(0)
    }
    val session = SparkSession
      .builder()
      .appName("NormV2")
    val env = args(0)
    var useIAM = true
    if (!"emr".equalsIgnoreCase(env)) {
      useIAM = false
      session
        .master("local[*]")
        .config("spark.hadoop.fs.s3a.access.key", Config().getString("normv2.accessKey"))
        .config("spark.hadoop.fs.s3a.secret.key", Config().getString("normv2.secretKey"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    }
    val spark = session
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Config().getString("normv2.loggerLevel"))



    var stTime = System.currentTimeMillis();


    val retailDataSchema = new StructType()
      .add("srno", IntegerType)
      .add("InvoiceNo", IntegerType)
      .add("StockCode", StringType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", DoubleType)
      .add("InvoiceTimestamp", TimestampType)
      .add("EventTimestamp", TimestampType)
      .add("NormalizedTimestamp", TimestampType)
      .add("UUID", StringType)

   // val userSchema = spark.read.parquet(Config().getString("normv2.sinkPath")+"parquet/*/*/*").schema
    val pq_df = spark.readStream
      .format("parquet")
      .schema(retailDataSchema)
      .load(Config().getString("normv2.sinkPath")+"parquet/")

    //println(userSchema)
    println("new schema")
    pq_df.schema
    //pq_df.show()
    val aggq =  pq_df.groupBy().count()
    val query = aggq.writeStream
      .format("console")
      .option("truncate", false)
      .queryName("s3ParquetStreamReader")
      .outputMode(OutputMode.Update())
      .start()
      .awaitTermination()

  }
}
