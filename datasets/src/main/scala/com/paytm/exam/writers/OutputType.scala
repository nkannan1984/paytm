package com.paytm.exam.writers

object OutputType extends Enumeration {
  type OutputType = Value

  val Parquet, CSV, JSON, JDBC = Value
}
