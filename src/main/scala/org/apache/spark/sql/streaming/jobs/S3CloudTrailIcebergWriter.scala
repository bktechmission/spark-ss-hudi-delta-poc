package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.utils.Config
import org.apache.spark.sql.types._

import java.util.concurrent.TimeUnit


object S3CloudTrailIcebergWriter extends Logging {

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
      .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.glue_catalog.warehouse", "s3://bhupitestproduswest2/trailsgluewh")
      .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
      .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Config().getString("normv2.loggerLevel"))
    spark.sqlContext.udf.register("uuid", uuid)

    val cloudTrailSchema = new StructType()
      .add("Records", ArrayType(new StructType()
        .add("additionalEventData", StringType)
        .add("apiVersion", StringType)
        .add("awsRegion", StringType)
        .add("errorCode", StringType)
        .add("errorMessage", StringType)
        .add("eventCategory", StringType)
        .add("eventID", StringType)
        .add("eventName", StringType)
        .add("eventSource", StringType)
        .add("eventTime", StringType)
        .add("eventType", StringType)
        .add("eventVersion", StringType)
        .add("managementEvent", BooleanType)
        .add("readOnly", BooleanType)
        .add("recipientAccountId", StringType)
        .add("requestID", StringType)
        .add("sharedEventID", StringType)
        .add("sourceIPAddress", StringType)
        .add("userAgent", StringType)
        .add("vpcEndpointId", StringType)))

    //E Extract: Read data from SNS SQS Streaming Source
    val rawRecords = spark.readStream
      .option("maxFilesPerTrigger", Config().getString("normv2.maxFilesPerTrigger"))
      .schema(cloudTrailSchema)
      .json(Config().getString("normv2.sourcePath"))

    //T Transform: Enrich Data
    /*
    Then, we are going to transform the data in the following way.
    1. Explode (split) the array of records loaded from each file into separate records.
    2. Parse the string event time string in each record to Spark’s timestamp type.
    3. Flatten out the nested columns for easier querying.
     */
    import spark.sqlContext.implicits._
    val cloudTrailEvents = rawRecords
      .select(explode($"Records") as "record")
      .select(
        unix_timestamp($"record.eventTime", "yyyy-MM-dd'T'HH:mm:ss'Z'").cast("timestamp") as "timestamp",
        $"record.*"
      )

    val augEvents = cloudTrailEvents
      .withColumn("date", $"timestamp".cast("date"))
      .withColumn("normalizedTimestamp", current_timestamp())
      .withColumn("uuid", uuid())
    //augEvents.show();
    // Create and start query, write in Delta
    //L Load: Loading Data back to Data Lake S3

    val streamingETLQuery = augEvents
      .writeStream
      .format("iceberg")
      .partitionBy("date", "eventSource")
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("checkpointLocation", Config().getString("normv2.checkpointLocation") + "ice_trails/")
      .option("path", "glue_catalog.cloudlogs.trailsgluetable")
      .option("fanout-enabled", "true")
      .start()
    streamingETLQuery.awaitTermination()
  }

  def uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)

}
