package org.apache.spark.sql.streaming.jobs

import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.MultiPartKeysValueExtractor
import org.apache.hudi.keygen.ComplexKeyGenerator
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.streaming.utils.Config
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.joda.time.LocalDateTime

object S3ToHudiOptStreamJob extends Logging {
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

    // Create and start query, write in Hudi
    //L Load: Loading Data back to Data Lake S3
    // Bulk Insert https://github.com/apache/hudi/issues/2639
    //https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-transparent-tpc-ds-lakehouse-performance-benchmarks

    val query = augdf
      .writeStream
      .format("hudi")
      .option(RECORDKEY_FIELD.key(), "UUID")
      .option(PRECOMBINE_FIELD.key(), "NormalizedTimestamp")
      .option(PARTITIONPATH_FIELD.key(), "Date,Country")
      .option(KEYGENERATOR_CLASS_NAME.key(), classOf[ComplexKeyGenerator].getName())
      .option("hoodie.datasource.write.row.writer.enable", "false")
      .option("hoodie.table.name", "defsec_hudis3list")
      .option("hoodie.datasource.write.table.name", "defsec_hudis3list")
      .option("hoodie.datasource.write.hive_style_partitioning", "true")
      .option("hoodie.datasource.write.operation", "bulk_insert")
      .option("hoodie.combine.before.insert", "false")
      .option("hoodie.bulkinsert.sort.mode", "NONE")
      .option("hoodie.parquet.compression.codec", "snappy")
      .option("hoodie.populate.meta.fields", "false")
      .option("hoodie.metadata.enable", "false")
      //.option("hoodie.parquet.max.file.size", "141557760") // 135Mb
      //.option("hoodie.parquet.block.size", "141557760") // 135Mb
      .queryName("s3ToHudiOptStreamJob")
      .option("checkpointLocation", Config().getString("normv2.checkpointLocation")+"hudis3list_opt/")
      .option("path", Config().getString("normv2.sinkPath")+"hudis3list_opt/")
      .outputMode(OutputMode.Append())
      .start()

    query.awaitTermination()
  }

  def uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)
}
