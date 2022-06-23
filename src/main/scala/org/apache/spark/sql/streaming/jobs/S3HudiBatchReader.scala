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
      .getOrCreate()
    spark.sparkContext.setLogLevel(Config().getString("normv2.loggerLevel"))
    import spark.sqlContext.implicits._
    var stTime = System.currentTimeMillis();
    val pq_df = spark.read
      .format("delta")
      .load(Config().getString("normv2.sinkPath")+"deltas3list_emr/")

    println("Total records pq_df.count "+ pq_df.count)
    val agg_pq = pq_df.groupBy($"StockCode").count()
    println("Total Stock Codes agg_pq.count: "+ agg_pq.count + " distinct:" + pq_df.select($"StockCode").distinct.count)
    var endTime = System.currentTimeMillis();
    println("Total parquet show time: "+ (endTime-stTime)/1000 + "seconds")
    println("Total pq_df srno count > 1: ")
    val srnoaggpq = pq_df.groupBy($"srno").count().where($"count">1)
    srnoaggpq.show()

    stTime = System.currentTimeMillis();
    val hudi_df = spark.read
      .format("org.apache.hudi")
      .load(Config().getString("normv2.sinkPath")+"hudis3list_opt_emr/")

    println("Total records hudi_df.count "+ hudi_df.count)
    val agg_hd = hudi_df.groupBy($"StockCode").count()
    println("Total Stock Codes agg_hd.count: "+ agg_hd.count + " distinct:" +hudi_df.select($"StockCode").distinct.count)
    endTime = System.currentTimeMillis();
    println("Total hudi show time: "+ (endTime-stTime)/1000 + "seconds")

    println("Total hudi_df srno count > 1: ")
    val srnoagghd = hudi_df.groupBy($"srno").count().where($"count">1)
    srnoagghd.show()

  }
}
