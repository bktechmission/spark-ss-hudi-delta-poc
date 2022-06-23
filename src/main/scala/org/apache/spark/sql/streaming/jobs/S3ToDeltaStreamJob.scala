package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.utils.Config
import org.apache.spark.sql.types._

object S3ToDeltaStreamJob extends Logging {

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
    spark.sqlContext.udf.register("uuid", uuid)

    val retailDataSchema = new StructType()
      .add("srno", IntegerType)
      .add("InvoiceNo", IntegerType)
      .add("StockCode", StringType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerID", DoubleType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)

    //E Extract: Read data from SNS SQS Streaming Source
    val strmdf = spark
      .readStream
      .format(Config().getString("normv2.sourceFormat"))
      .schema(retailDataSchema)
      .option("maxFilesPerTrigger", Config().getString("normv2.maxFilesPerTrigger"))
      .option("header", "true")
      .load(Config().getString("normv2.sourcePath"))

    //T Transform: Enrich Data
    import spark.sqlContext.implicits._
    //val filteredData = streamingData.filter("Country = 'United Kingdom'")
    val augdf = strmdf
      .withColumn("EventTimestamp", to_timestamp($"InvoiceTimestamp", "yyyy-MM-dd HH24:mm:ss"))
      .withColumn("Date", to_date($"EventTimestamp"))
      .withColumn("NormalizedTimestamp", current_timestamp())
      .withColumn("UUID", uuid())

    // Create and start query, write in 2 modes Plain Parquet and Hudi
    //L Load: Loading Data back to Data Lake S3

   val query = augdf
      .writeStream
     .format("delta")
     .partitionBy("Date", "Country")
     .option("checkpointLocation", Config().getString("normv2.checkpointLocation")+"deltas3list_emr/")
     .option("path", Config().getString("normv2.sinkPath")+"deltas3list_emr/")
     .outputMode(OutputMode.Append())
     .start()
    spark.streams.awaitAnyTermination()
  }

  def uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)

}
