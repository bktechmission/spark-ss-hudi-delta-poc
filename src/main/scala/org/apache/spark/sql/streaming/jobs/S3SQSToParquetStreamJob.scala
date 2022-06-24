package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{current_timestamp, to_date, to_timestamp, udf}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.utils.Config
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object S3SQSToParquetStreamJob extends Logging {
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
      .config("spark.dynamicAllocation.enabled", "false")
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
      .format("s3-sqs")
      .schema(retailDataSchema)
      .option("sqsUrl", Config().getString("normv2.sqsQueue"))
      .option("fileFormat", Config().getString("normv2.sourceFormat"))
      .option("region", Config().getString("normv2.region"))
      .option("header", "true")
      .option("sqsFetchIntervalSeconds", Config().getString("normv2.sqsFetchIntervalSeconds"))
      .option("useInstanceProfileCredentials", useIAM)
      .option("sqsLongPollingWaitTimeSeconds", Config().getString("normv2.sqsLongPollingWaitTimeSeconds"))
      .option("messageWrapper", "SNS")
      .option("maxFilesPerTrigger", Config().getString("normv2.maxFilesPerTrigger"))
      .load()

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
      .format("parquet")
      .partitionBy("Date", "Country")
      .option("checkpointLocation", Config().getString("normv2.checkpointLocation")+"parquets3sqs_emr/")
      .option("path", Config().getString("normv2.sinkPath")+"parquets3sqs_emr")
      .outputMode(OutputMode.Append())
      .start()
    spark.streams.awaitAnyTermination()
  }

  def uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)
}
