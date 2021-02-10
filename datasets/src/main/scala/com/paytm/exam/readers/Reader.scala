package com.paytm.exam.readers

import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

trait Reader {

  def read(SQLContext: SQLContext): DataFrame
}
