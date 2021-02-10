package com.paytm.exam.common

import org.apache.spark.sql.SparkSession

object SparkConnection {
  var spark: SparkSession = null

  def getSparkContext: SparkSession = {
    if (spark == null) getConnection()
    spark
  }

  private def getConnection(): Unit = {
    spark = SparkSession.builder.appName("exam-processing").getOrCreate
    spark.conf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  }
}
