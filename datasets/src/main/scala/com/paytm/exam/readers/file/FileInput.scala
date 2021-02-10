package com.paytm.exam.readers.file

import com.paytm.exam.readers.Reader
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

case class FileInput(path: String,
                     options: Option[Map[String, String]],
                     format: Option[String])
    extends Reader {
  def read(SQLContext: SQLContext): DataFrame = {
    FilesInput(path.split(","), options, format).read(SQLContext)
  }
}
