package com.paytm.exam.transformations

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

trait Transform {

  def transformation(context: SparkContext, sqlContext: SQLContext)
}
