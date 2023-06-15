package org.apache.spark.sql.streaming.jobs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{count, to_json, udf}
import org.apache.spark.sql.streaming.jobs.S3ToHudiStreamJob.uuid
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}

object StringTransformJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Structured Streaming")
      .master("local[*]")
      .getOrCreate()
    spark.sqlContext.udf.register("uuid", uuid)
    spark.sparkContext.setLogLevel("ERROR")

/*
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
      .add("InvoiceTimestamp", TimestampType)*/
    import spark.sqlContext.implicits._
    val path = "/Users/bpanwar/Desktop/file1.txt"
    val df9 = spark.read.textFile(path)
    df9.show(false)
    val df6 = spark.read.option("mergeSchema", true).json(df9)
    df6.printSchema()
    df6.show(false)

    val jsonString1 = """{"Zipcode":704,"ZipCodeType":"STANDARD1","City":"PARC PARQUE1","State":"PR1"}"""
    val jsonString2 = """{"Zipcode":705,"ZipCodeType":"STANDARD2","City":"PARC PARQUE2","StateMap": {"State":"PR2"}}"""
    val jsonString3 = """{"Zipcode":706,"ZipCodeType":"STANDARD3","City":"PARC PARQUE3","StateMap": {"State":"PR3"}}"""
    val jsonString4 = """{"Zipcode":707,"ZipCodeType":"STANDARD4","CityArr":["PARC PARQUE4", "PARQUE4"],"StateMap": {"State":"PR4"}}"""

    val data = Seq((1, jsonString1),(2, jsonString2),(3, jsonString3),(4, jsonString4))
    import org.apache.spark.sql.functions.{from_json, col}
    val df = data.toDF("id", "value")
    df.show()
    df.printSchema()
    val uberLevelSchema = spark.read.json(df.select("value").as[String]).schema
    val df2 = df.withColumn("value",
      from_json(col("value"), uberLevelSchema))
    df2.show()
    df2.printSchema()
    val df3 = df2.select("*", "value.*").drop("value")
    df3.show(false)

    df3.write.json("/Users/bpanwar/Desktop/sparkout/")
   /*
    import org.apache.spark.sql.types.{MapType, StringType}
    val df2 = df.withColumn("value", from_json(col("value"), MapType(StringType, StringType)))
    df2.printSchema()
    df2.show(false)

    val json_col = to_json($"value")
    val json_schema = spark.read.json(df2.select(json_col).as[String]).schema
    val df3 = df2.withColumn("value", from_json(json_col, json_schema))
    val df4 = df3.select("*", "value.*").drop("value")
    df4.show()*/
  }

  def uuid: UserDefinedFunction = udf(() => java.util.UUID.randomUUID().toString)
}
