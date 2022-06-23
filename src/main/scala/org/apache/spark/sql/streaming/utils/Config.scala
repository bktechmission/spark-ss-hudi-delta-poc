package org.apache.spark.sql.streaming.utils

import com.typesafe.config._

object Config {
  val env = if (System.getenv("ENVIRONMENT") == null) "development" else System.getenv("ENVIRONMENT")

  val conf = ConfigFactory.load()
  def apply() = conf.getConfig(env)
}