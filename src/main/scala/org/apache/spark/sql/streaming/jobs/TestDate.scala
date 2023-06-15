package jobs
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, hour, minute, second, to_date, to_timestamp}

object TestDate extends App {

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  val df = Seq(("2019-07-01 12:01:19.000"),
    ("2023-01-24T01:59:00.839Z"),
    ("2019-11-16 16:44:55.406"),
    ("2023-01-17T22:30:40.490Z")).toDF("input_timestamp")

  df.withColumn("date", to_date(col("input_timestamp")))
    .withColumn("hour", hour(to_timestamp($"input_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))
    .withColumn("minute", minute(col("input_timestamp")))
    .withColumn("second", second(col("input_timestamp")))
    .show(false)

}