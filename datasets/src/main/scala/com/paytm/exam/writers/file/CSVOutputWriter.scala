package com.paytm.exam.writers.file

import com.paytm.exam.writers.Writer
import org.apache.spark.sql.DataFrame

class CSVOutputWriter(var props: Map[String, String]) extends Writer {
  props = Option(props).getOrElse(Map())

  val defaultCSVOptions = Map("escape" -> "\"")
  // Backward compatibility
  val csvOptions =
    props.getOrElse("csvOptions", Map.empty).asInstanceOf[Map[String, String]]
  //val headerOptions = props.getOrElse("headerOptions",Map.empty).asInstanceOf[Map[String, String]]
  val extraOptions =
    props.getOrElse("extraOptions", Map.empty).asInstanceOf[Map[String, String]]
  val options = defaultCSVOptions ++ csvOptions ++ extraOptions

  val fileOutputWriter = new FileOutputWriter(
    props + (
      "extraOptions" -> options,
      "format" -> "com.databricks.spark.csv",
      "header" -> "true"
    )
  )

  override def write(dataFrame: DataFrame): Unit = {
    fileOutputWriter.write(dataFrame)
  }

}
