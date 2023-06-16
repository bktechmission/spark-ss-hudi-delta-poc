package org.apache.spark.sql.streaming.jobs;

import org.apache.spark.sql.*;
import scala.collection.immutable.Map;

import java.util.ArrayList;
import java.util.List;

public class TestDateJava {

    public static void main(String[] args) {
        final SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("SparkByExamples.com")
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");
        /*/

        Dataset<String> datedf = new Sequence(("2019-07-01 12:01:19.000"),
                ("2023-01-24T01:59:00.839Z"),
                ("2019-11-16 16:44:55.406"),
                ("2023-01-17T22:30:40.490Z")).toDF("input_timestamp");
        */
        List<String> data = new ArrayList<>();
        data.add("2019-07-01 12:01:19.000");
        data.add("2023-01-24T01:59:00.839Z");
        data.add("2019-11-16 16:44:55.406");
        data.add("2023-01-17T22:30:40.490Z");
        // DataFrame
        Map<String, String> sc = spark.conf().getAll();
        sc.toStream().print();
        Dataset<Row> datedf = spark.createDataset(data, Encoders.STRING()).toDF("input_timestamp");
        datedf.withColumn("date", functions.to_date(datedf.col("input_timestamp")))
                .withColumn("ts", functions.to_timestamp(datedf.col("input_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
                .withColumn("hour", functions.hour(functions.to_timestamp(datedf.col("input_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")))
                .withColumn("minute", functions.minute(datedf.col("input_timestamp")))
                .withColumn("second", functions.second(datedf.col("input_timestamp")))
                .show(false);
    }
}
