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
    //val jsonString1 = """{"Zipcode":704,"ZipCodeType":"STANDARD1","City":"PARC PARQUE1","State":"PR1"}"""
     // val jsonString2 = """{"Zipcode":705,"ZipCodeType":"STANDARD2","City":"PARC PARQUE2","State":"PR2"}"""
    //val data = Seq((1, jsonString1)).add(2, jsonString2)
    val df = spark.read.textFile(path)
    df.show(false)
    val df6 = spark.read.option("mergeSchema", true).json(df)
    df6.printSchema()
    df6.show(false)

   /* import org.apache.spark.sql.functions.{from_json, col}
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
