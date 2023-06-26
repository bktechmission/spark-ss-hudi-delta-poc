package org.apache.spark.sql.streaming.sqs

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Source
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSourceProvider}
import org.apache.spark.sql.types.StructType

class SqsSourceProvider extends DataSourceRegister
  with StreamSourceProvider
  with LazyLogging {

  override def shortName(): String = "s3-sqs"

  override def sourceSchema(sqlContext: SQLContext,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): (String, StructType) = {

    require(schema.isDefined, "Sqs source doesn't support empty schema")
    (shortName(), schema.get)
  }

  override def createSource(sqlContext: SQLContext,
                            metadataPath: String,
                            schema: Option[StructType],
                            providerName: String,
                            parameters: Map[String, String]): Source = {

    new SqsSource(
      sqlContext.sparkSession,
      metadataPath,
      parameters,
      schema.get)
  }
}
