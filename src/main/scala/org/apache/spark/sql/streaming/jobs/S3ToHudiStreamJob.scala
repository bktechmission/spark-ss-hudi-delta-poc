package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.streaming.OutputMode
import org.joda.time.LocalDateTime
import org.apache.spark.sql.streaming.utils.Config

object S3ToHudiStreamJob extends Logging {
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
      //.add("UUID", StringType)

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
      .format("hudi")
      .option("hoodie.table.name", "defsec_hudis3list")
      .option(RECORDKEY_FIELD.key, "UUID")
      .option(PRECOMBINE_FIELD.key, "NormalizedTimestamp")
      .option(PARTITIONPATH_FIELD.key, "Date,Country")
      .option(STREAMING_IGNORE_FAILED_BATCH.key, false)
      .option(KEYGENERATOR_CLASS_NAME.key, classOf[ComplexKeyGenerator].getName)
      .option(HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY, classOf[MultiPartKeysValueExtractor].getName)
      .option(HIVE_PARTITION_FIELDS.key, "Date,Country")
      .option(STREAMING_RETRY_CNT.key, 0)
      .queryName("s3ToHudiStreamJob")
      .option("checkpointLocation", Config().getString("normv2.checkpointLocation")+"hudis3list/")
      .option("path", Config().getString("normv2.sinkPath")+"hudis3list/")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

  def persist_DF( batchDF:DataFrame, batchID:Long ) : Unit = {
    val hudiOptions = Map[String,String](
      HoodieWriteConfig.TABLE_NAME -> "defsec.invoices_test_table",
      TABLE_TYPE_OPT_KEY -> "COPY_ON_WRITE",
      RECORDKEY_FIELD_OPT_KEY -> "UUID",
      PRECOMBINE_FIELD_OPT_KEY -> "NormalizedTimestamp",
      PARTITIONPATH_FIELD_OPT_KEY -> "Date,Country",
      KEYGENERATOR_CLASS_OPT_KEY -> classOf[ComplexKeyGenerator].getName,
      HIVE_STYLE_PARTITIONING_OPT_KEY -> "true",
      HIVE_SYNC_ENABLED_OPT_KEY -> "false",
      HIVE_TABLE_OPT_KEY -> "defsec.invoices_test_table",
      HIVE_PARTITION_FIELDS_OPT_KEY -> "Date,Country",
      HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY -> classOf[MultiPartKeysValueExtractor].getName
    )

    // First persist, same DF will be used 2 times
    batchDF.persist()

    println(LocalDateTime.now() + "Start writing Hudi table")
    batchDF.write
      .format("hudi")
      .options(hudiOptions)
      .mode(SaveMode.Append)
      .save(Config().getString("normv2.sinkPath")+"hudi")

    // un mount the persist state
    batchDF.unpersist()
  }

  def uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)
}
