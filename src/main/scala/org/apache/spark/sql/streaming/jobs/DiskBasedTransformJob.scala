package org.apache.spark.sql.streaming.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{count, udf}
import org.apache.spark.sql.streaming.jobs.S3ToHudiStreamJob.uuid
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object DiskBasedTransformJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Structured Streaming")
      .master("local[*]")
      .getOrCreate()
    spark.sqlContext.udf.register("uuid", uuid)
    spark.sparkContext.setLogLevel("ERROR")


    val retailDataSchema = new StructType()
      .add("srno", IntegerType)
      .add("InvoiceNo", IntegerType)
      .add("StockCode", IntegerType)
      .add("Description", StringType)
      .add("Quantity", IntegerType)
      .add("InvoiceDate", TimestampType)
      .add("UnitPrice", DoubleType)
      .add("CustomerId", IntegerType)
      .add("Country", StringType)
      .add("InvoiceTimestamp", TimestampType)
    import spark.sqlContext.implicits._
    val strmdf = spark
      .read
      .format("csv")
      .option("header", true)
      .schema(retailDataSchema)
      .option("maxFilesPerTrigger", "100")
      //.option("path", "/Users/bpanwar/temp_working/input/2010-12-01/*/")
      .option("path", "/Users/bpanwar/temp_working/input/4files/")
      .load()
    println("Total rows "+strmdf.count)
    val agg_df = strmdf.groupBy($"StockCode").count()
    println("Total Stock Codes agg_df.count: "+ agg_df.count + " distinct:" +strmdf.select($"StockCode").distinct.count)

    val srnoagg = strmdf.groupBy($"srno").count().where($"count">=4)
    srnoagg.show()
    //val filteredData = streamingData.filter("Country = 'United Kingdom'")
    /*import spark.sqlContext.implicits._
    val augdf = strmdf
                .withColumn("ts", to_timestamp($"InvoiceTimestamp", "yyyy-MM-dd HH24:mm:ss"))
                .withColumn("AtYear", date_trunc("year", $"ts"))
                .withColumn("AtMon", date_trunc("month", $"ts"))
                .withColumn("AtDay", to_date($"ts"))
                .withColumn("AtHr", date_trunc("hour", $"ts"))
                .withColumn("yy", year($"ts"))
                .withColumn("mo", month($"ts"))
                .withColumn("dd", dayofmonth($"ts"))
                .withColumn("hh", hour($"ts"))
                .withColumn("mm", minute($"ts"))
                .withColumn("ss", expr("current_timestamp - Interval 1 days"))
      .withColumn("unix_time", date_format(to_timestamp($"InvoiceTimestamp"), "yyyy/MM/dd'T'hh:mm:ss.SSS'Z'"))
      .withColumn("other_time", to_timestamp($"unix_time", "yyyy/MM/dd'T'hh:mm:ss.SSS'Z'"))
      .withColumn("pst_ts", to_timestamp(current_timestamp()))
      .withColumn("utc_ts", to_utc_timestamp($"pst_ts", "MST"))
      .withColumn("epoch1", unix_timestamp($"pst_ts"))
      .withColumn("epoch2", unix_timestamp($"utc_ts"))
      .withColumn("fromepc1", from_unixtime($"epoch1"))
      .withColumn("fromepc2", from_unixtime($"epoch2"))

    val query = augdf.writeStream
      .format("console")
      .option("truncate", false)
      .queryName("filteredByCountry")
      .outputMode(OutputMode.Update())
      .start()
*/
     /*     strmdf
              .withColumn("UUID", uuid())
              .write
              .format("csv")
              .option("header", true)
              .option("checkpointLocation", "/Users/bpanwar/temp_working/checkpoint/")
              .option("path","/Users/bpanwar/temp_working/output/")
            .save()*/
    //query.awaitTermination()
  }

  def uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)
}
