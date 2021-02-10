package com.paytm.exam.executor

import com.paytm.exam.common.SparkConnection
import com.paytm.exam.transformations.Impl.WeatherTransformation
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.slf4j.LoggerFactory

object MainJob {
  val logger = LoggerFactory.getLogger(this.getClass)
  def main(args: Array[String]): Unit = {

    val spark = SparkConnection.getSparkContext
    val sqlcontext = spark.sqlContext
    val sparkcontext = spark.sparkContext

    WeatherTransformation.transformation(sparkcontext, sqlcontext);

  }

}
