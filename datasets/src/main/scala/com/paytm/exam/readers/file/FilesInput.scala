package com.paytm.exam.readers.file

import com.paytm.exam.readers.Reader
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

case class FilesInput(paths: Seq[String],
                      options: Option[Map[String, String]],
                      format: Option[String])
    extends Reader
    with FileInputBase {
  def read(SQLContext: SQLContext): DataFrame = {
    val readFormat = getFormat(format, paths.head)

    val reader = SQLContext.read.format(readFormat)

    val readOptions = getOptions(readFormat, options)

    readOptions match {
      case Some(opts) => reader.options(opts)
      case None       =>
    }

    val df = reader.load(paths: _*)

    processDF(df, readFormat)
  }
}
