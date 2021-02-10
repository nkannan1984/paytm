package com.paytm.exam.readers.file

import org.apache.spark.sql.DataFrame

trait FileInputBase {

  def getFormat(format: Option[String], path: String): String = {

    format match {
      case Some(f) => {
        f match {
          case "avro" => "com.databricks.spark.avro"
          case _      => f
        }
      }
      case None => {
        FileType.getFileType(path) match {
          case FileType.json | FileType.jsonl => "json"
          case FileType.csv                   => "csv"

          case _ => "parquet"
        }
      }
    }
  }

  // set default options for file types here
  def getOptions(
    readFormat: String,
    options: Option[Map[String, String]]
  ): Option[Map[String, String]] = {
    readFormat match {
      case "csv" => {
        Option(Map("header" -> "true") ++ options.getOrElse(Map()))
      }
      case _ => options
    }
  }

  def processDF(df: DataFrame, readFormat: String): DataFrame = {
    readFormat match {
      case "csv" => df
      case _     => df
    }
  }

}
