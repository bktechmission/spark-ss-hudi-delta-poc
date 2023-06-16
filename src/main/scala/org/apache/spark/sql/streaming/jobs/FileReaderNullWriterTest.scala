package org.apache.spark.sql.streaming.jobs

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}

import java.io.File
import java.nio.file.StandardCopyOption._
import java.nio.file.{Files, Paths}
import java.util


object FileReaderNullWriterTest extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local")
    .appName("SparkByExamples.com")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  import spark.sqlContext.implicits._

  val empDataSchema = new StructType()
    .add("fname", StringType, true)
    .add("lname", StringType, true)
    .add("dept", StringType, true)
    .add("phone", StringType, true)
    .add("city", StringType, true)
    .add("state", StringType, true)

  //E Extract: Read data from SNS SQS Streaming Source
  val strmdf = spark
    .read
    .format("json")
    .schema(empDataSchema)
    //.option("maxFilesPerTrigger", Config().getString("normv2.maxFilesPerTrigger"))
    //.option("header", "true")
    .load("/Users/bpanwar/Desktop/sparktest/raw_data/*")

  //T Transform: Enrich Data

  import spark.sqlContext.implicits._
  strmdf.show(false)
  val repartDF = strmdf
    .repartition(strmdf.col("dept"))
  val dfQuery = strmdf
    .write
    .mode(SaveMode.Overwrite)
    .format("delta")
    .partitionBy("dept")
    .save("/Users/bpanwar/Desktop/sparktest/normdata")

  var f = new File("/Users/bpanwar/Desktop/mytest1/splits/ssh/userid.txt");
  f.getParentFile.mkdirs();
  f.createNewFile();
  f = new File("/Users/bpanwar/Desktop/mytest1/splits/ssh/email.txt");
  f.getParentFile.mkdirs();
  f.createNewFile();
  f = new File("/Users/bpanwar/Desktop/mytest1/splits/ssh/timestamp.txt");
  f.getParentFile.mkdirs();
  f.createNewFile();
  f = new File("/Users/bpanwar/Desktop/mytest1/splits/bro/fname1.txt");
  f.getParentFile.mkdirs();
  f.createNewFile();
  f = new File("/Users/bpanwar/Desktop/mytest1/splits/bro/lname1.txt");
  f.getParentFile.mkdirs();
  f.createNewFile();
  f = new File("/Users/bpanwar/Desktop/mytest1/splits/bro/email.txt");
  f.getParentFile.mkdirs();
  f.createNewFile();
  buildMergedSchema("/Users/bpanwar/Desktop/mytest1");

  val yourFile = Paths.get("/Users/bpanwar/Desktop/sparktest/renametest/data1.txt-temp")

  Files.move(yourFile, yourFile.resolveSibling("data1.txt"), REPLACE_EXISTING)

  //val filteredData = streamingData.filter("Country = 'United Kingdom'")
  /*val augdf = strmdf
    .withColumn("EventTimestamp", to_timestamp($"InvoiceTimestamp", "yyyy-MM-dd HH24:mm:ss"))
    .withColumn("Date", to_date($"EventTimestamp"))
    .withColumn("NormalizedTimestamp", current_timestamp())
    .withColumn("UUID", uuid())*/

  // Create and start query, write in Delta
  //L Load: Loading Data back to Data Lake S3

  /*val query = augdf
    .writeStream
    .format("delta")
    .partitionBy("Date", "Country")
    .trigger(Trigger.Once())
    .option("checkpointLocation", Config().getString("normv2.checkpointLocation") + "deltas3list/")
    .option("path", Config().getString("normv2.sinkPath") + "deltas3list/")
    .outputMode(OutputMode.Append())
    .start()*/
  //spark.streams.awaitAnyTermination()
  private def buildMergedSchema(baseSchemaDirPath: String): Unit = {
    val allSchemas: util.Map[String, StructType] = new util.HashMap[String, StructType]
    val schemaDir: File = new File(baseSchemaDirPath + "/splits")
    val sourceTypesFolders: Array[File] = schemaDir.listFiles
    for (eachSourceTypeFolder <- sourceTypesFolders) {
      // create each sourceType schema
      val fieldNameFiles: Array[File] = eachSourceTypeFolder.listFiles
      val srcTypeName: String = eachSourceTypeFolder.getName
      println(srcTypeName)
      for (fieldNameFile <- fieldNameFiles) {
        println(fieldNameFile.getName)
      }
    }


  }
}