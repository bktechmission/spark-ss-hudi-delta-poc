package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.utils.Config


object S3ParquetBatchReader extends Logging {
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
    import spark.sqlContext.implicits._

    ////////////////// Plain Parquet Reader    ////////////////////////////
    var stTime = System.currentTimeMillis();
    val parq_df = spark.read
      .format("parquet")
      .load(Config().getString("normv2.sinkPath")+"parquets3list/")
    var endTime = System.currentTimeMillis();
    println("Total PARQUET LOAD time: " + (endTime - stTime) / 1000 + "seconds")

    stTime = System.currentTimeMillis();
    println("Records parq_df.count: " + parq_df.count)
    endTime = System.currentTimeMillis();
    println("Time taken by PARQUET parq_df.count: " + (endTime - stTime) / 1000 + "seconds")

    stTime = System.currentTimeMillis();
    val agg_parq_count = parq_df.groupBy($"StockCode").count().count()
    println("Records parq_df.groupBy($StockCode).count().count(): " + agg_parq_count)
    endTime = System.currentTimeMillis();
    println("Time taken by PARQUET parq_df.groupBy($StockCode).count().count(): " + (endTime - stTime) / 1000 + "seconds")

    stTime = System.currentTimeMillis();
    val parq_distinct_count = parq_df.select($"StockCode").distinct.count
    println("Records parq_df.select($StockCode).distinct.count : " + parq_distinct_count)
    endTime = System.currentTimeMillis();
    println("Time taken by PARQUET parq_df.select($StockCode).distinct.count: " + (endTime - stTime) / 1000 + "seconds")

    stTime = System.currentTimeMillis();
    println("Total parq_df srno count > 1: ")
    val srnoaggparq = parq_df.groupBy($"srno").count().where($"count" > 1)
    srnoaggparq.show()
    endTime = System.currentTimeMillis();
    println("Time taken by PARQUET parq_df.groupBy($srno).count().where($count > 1) : " + (endTime - stTime) / 1000 + "seconds")


  }
}
