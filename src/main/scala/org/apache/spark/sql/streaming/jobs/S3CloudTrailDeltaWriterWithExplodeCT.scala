package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.utils.Config
import org.apache.spark.sql.types._


object S3CloudTrailDeltaWriterWithExplodeInRawCT extends Logging {

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
        .config("spark.hadoop.fs.s3a.session.token", Config().getString("normv2.sessionToken"))
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    }
    val spark = session
      .config("spark.hadoop.fs.s3a.endpoint", "s3.us-west-2.amazonaws.com")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.AbstractFileSystem.s3.impl", "org.apache.hadoop.fs.s3a.S3A")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      //.config("spark.dynamicAllocation.enabled", "false")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Config().getString("normv2.loggerLevel"))
    spark.sqlContext.udf.register("uuid", uuid)
    /*
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

     */

    //https://github.com/databrickslabs/splunk-integration/blob/master/notebooks/source/cloudtrail_ingest.py
    val cloudTrailSchema =
      new StructType()
        .add("Records", ArrayType(new StructType()
          .add("eventTime", StringType)
          .add("eventVersion", StringType)
          .add("eventSource", StringType)
          .add("eventName", StringType)
          .add("eventType", StringType)
          .add("eventID", StringType)
          .add("eventCategory", StringType)
          .add("userIdentity", new StructType()
            .add("type", StringType)
            .add("userName", StringType)
            .add("principalId", StringType)
            .add("arn", StringType)
            .add("accountId", StringType)
            .add("accessKeyId", StringType)
            .add("invokedBy", StringType)
            .add("identityProvider", StringType)
            .add("sessionContext", new StructType()
              .add("attributes", new StructType()
                .add("creationDate", StringType)
                .add("mfaAuthenticated", StringType)))
            .add("sessionIssuer", new StructType()
              .add("accountId", StringType)
              .add("arn", StringType)
              .add("principalId", StringType)
              .add("type", StringType)
              .add("userName", StringType))
            .add("webIdFederationData", new StructType()
              .add("federatedProvider", StringType)
              .add("attributes", MapType(StringType, StringType))))
          .add("awsRegion", StringType)
          .add("sourceIPAddress", StringType)
          .add("userAgent", StringType)
          .add("errorCode", StringType)
          .add("errorMessage", StringType)
          .add("requestParameters", MapType(StringType, StringType))
          .add("responseElements", MapType(StringType, StringType))
          .add("additionalEventData", MapType(StringType, StringType))
          .add("requestID", StringType)
          .add("readOnly", BooleanType)
          .add("apiVersion", StringType)
          .add("managementEvent", BooleanType)
          .add("recipientAccountId", StringType)
          .add("vpcEndpointId", StringType)
          .add("serviceEventDetails", MapType(StringType, StringType))
          .add("sharedEventID", StringType)
          .add("resources", ArrayType(MapType(StringType, StringType)))
          .add("sessionCredentialFromConsole", StringType)
          .add("edgeDeviceDetails", StringType)
          .add("tlsDetails", new StructType()
            .add("tlsVersion", StringType)
            .add("cipherSuite", StringType)
            .add("clientProvidedHostHeader", StringType))
          .add("addendum", new StructType()
            .add("reason", StringType)
            .add("updatedFields", StringType)
            .add("originalRequestID", StringType)
            .add("originalEventID", StringType))))

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
        $"record.eventTime" as "ddi_eventtimestamp",
        $"record.eventSource" as "subtype",
        $"record.awsRegion" as "ddi_awsreg",
        $"record"
      )

    val augEvents = cloudTrailEvents
      .withColumn("date", $"timestamp".cast("date"))
      .withColumn("ddi_normalizedTimestamp", current_timestamp().cast("string"))
      .withColumn("index",  lit("cloudtrail"))
      .withColumn("bu",  lit("falcon"))
      .withColumn("rawmsg", struct($"record.*"))

    //augEvents.printSchema()
    //augEvents.show()
    // Create and start query, write in Delta
    //L Load: Loading Data back to Data Lake S3

    val streamingETLQuery = augEvents
      .drop("timestamp", "record")
      .writeStream
      .format("json")
      .partitionBy("index", "date", "subtype", "bu")
      //.trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("checkpointLocation", Config().getString("normv2.checkpointLocation") + "delta_trails/")
      .option("path", Config().getString("normv2.sinkPath") + "delta_trails/")
      .start()
    streamingETLQuery.awaitTermination()
  }

  def uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)

}
