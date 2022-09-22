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

    // delta read
    var stTime = System.currentTimeMillis();
    val delta_df = spark.read
      .format("delta")
      .load(Config().getString("normv2.sinkPath")+"deltas3list/")

    println("Total records delta_df.count "+ delta_df.count)
    val agg_delta = delta_df.groupBy($"StockCode").count()
    println("Total Stock Codes agg_delta.count: "+ agg_delta.count + " distinct:" + delta_df.select($"StockCode").distinct.count)
    var endTime = System.currentTimeMillis();
    println("Total delta show time: "+ (endTime-stTime)/1000 + "seconds")
    println("Total delta_df srno count > 1: ")
    val srnoaggdelta = delta_df.groupBy($"srno").count().where($"count">1)
    srnoaggdelta.show()

    stTime = System.currentTimeMillis();
    val hudi_df = spark.read
      .format("org.apache.hudi")
      .load(Config().getString("normv2.sinkPath")+"hudis3list_opt/")

    println("Total records hudi_df.count "+ hudi_df.count)
    val agg_hd = hudi_df.groupBy($"StockCode").count()
    println("Total Stock Codes agg_hd.count: "+ agg_hd.count + " distinct:" +hudi_df.select($"StockCode").distinct.count)
    endTime = System.currentTimeMillis();
    println("Total hudi show time: "+ (endTime-stTime)/1000 + "seconds")

    println("Total hudi_df srno count > 1: ")
    val srnoagghd = hudi_df.groupBy($"srno").count().where($"count">1)
    srnoagghd.show()


    stTime = System.currentTimeMillis();
    val parq_df = spark.read
      .format("parquet")
      .load(Config().getString("normv2.sinkPath")+"parquets3list/")

    println("Total records parq_df.count "+ parq_df.count)
    val agg_parq = parq_df.groupBy($"StockCode").count()
    println("Total Stock Codes agg_parq.count: "+ agg_parq.count + " distinct:" +parq_df.select($"StockCode").distinct.count)
    endTime = System.currentTimeMillis();
    println("Total parquet show time: "+ (endTime-stTime)/1000 + "seconds")

    println("Total parq_df srno count > 1: ")
    val srnoaggparq= parq_df.groupBy($"srno").count().where($"count">1)
    srnoaggparq.show()

  }
}
