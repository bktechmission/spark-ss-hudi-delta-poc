package org.apache.spark.sql.streaming.jobs

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.utils.Config


object S3HudiBatchReader extends Logging {
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

    //////////////////////////// HUDI Reader ///////////////////////////////
    var stTime = System.currentTimeMillis();
    val hudi_df = spark.read
      .format("org.apache.hudi")
      .load(Config().getString("normv2.sinkPath")+"hudis3list_opt/")
    var endTime = System.currentTimeMillis();
    println("Total HUDI LOAD time: " + (endTime - stTime) / 1000 + "seconds")

    stTime = System.currentTimeMillis();
    println("Records hudi_df.count: " + hudi_df.count)
    endTime = System.currentTimeMillis();
    println("Time taken by HUDI hudi_df.count: " + (endTime - stTime) / 1000 + "seconds")

    stTime = System.currentTimeMillis();
    val agg_hudi_count = hudi_df.groupBy($"StockCode").count().count()
    println("Records hudi_df.groupBy($StockCode).count().count(): " + agg_hudi_count)
    endTime = System.currentTimeMillis();
    println("Time taken by HUDI hudi_df.groupBy($StockCode).count().count(): " + (endTime - stTime) / 1000 + "seconds")

    stTime = System.currentTimeMillis();
    val hudi_distinct_count = hudi_df.select($"StockCode").distinct.count
    println("Records hudi_df.select($StockCode).distinct.count : " + hudi_distinct_count)
    endTime = System.currentTimeMillis();
    println("Time taken by HUDI hudi_df.select($StockCode).distinct.count: " + (endTime - stTime) / 1000 + "seconds")

    stTime = System.currentTimeMillis();
    println("Total hudi_df srno count > 1: ")
    val srnoagghudi = hudi_df.groupBy($"srno").count().where($"count" > 1)
    srnoagghudi.show()
    endTime = System.currentTimeMillis();
    println("Time taken by HUDI hudi_df.groupBy($srno).count().where($count > 1) : " + (endTime - stTime) / 1000 + "seconds")

  }
}
