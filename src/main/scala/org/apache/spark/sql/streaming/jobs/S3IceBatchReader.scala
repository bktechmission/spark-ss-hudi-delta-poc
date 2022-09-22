package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.utils.Config


object S3IceBatchReader extends Logging {
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
      .config("spark.sql.catalog.glue_catalog.warehouse", "s3://bhupiiceberg/csvglue")
      .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
      .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .getOrCreate()
    spark.sparkContext.setLogLevel(Config().getString("normv2.loggerLevel"))
    import spark.sqlContext.implicits._

    // delta read
    val stTime = System.currentTimeMillis();
    val ice_df = spark.read
      .format("iceberg")
      .load("glue_catalog.retaildb.csvgluetable")

    println("Total records ice_df.count " + ice_df.count)
    val agg_ice = ice_df.groupBy($"StockCode").count()
    println("Total Stock Codes agg_ice.count: " + agg_ice.count + " distinct:" + ice_df.select($"StockCode").distinct.count)
    val endTime = System.currentTimeMillis();
    println("Total iceberg show time: " + (endTime - stTime) / 1000 + "seconds")

    println("Total ice_df srno count > 1: ")
    val srnoaggice = ice_df.groupBy($"srno").count().where($"count" > 1)
    srnoaggice.show()

  }
}
