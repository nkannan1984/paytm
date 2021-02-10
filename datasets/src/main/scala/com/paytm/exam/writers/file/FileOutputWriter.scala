package com.paytm.exam.writers.file

import com.paytm.exam.exceptions.GeneralException
import com.paytm.exam.writers.Writer
import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SparkSession}

class FileOutputWriter(props: Map[String, Object]) extends Writer {
  val log = LogManager.getLogger(this.getClass)
  val fileOutputProperties = FileOutputProperties(
    props.get("path").asInstanceOf[Option[String]],
    props.get("createUniquePath").asInstanceOf[Option[Boolean]],
    props.get("saveMode").asInstanceOf[Option[String]],
    props.get("partitionBy").asInstanceOf[Option[Seq[String]]],
    props.get("format").asInstanceOf[Option[String]],
    props.get("delimiter").asInstanceOf[Option[String]],
    props.get("header").asInstanceOf[Option[String]],
    props.get("quoteAll").asInstanceOf[Option[String]],
    props.get("protectFromEmptyOutput").asInstanceOf[Option[Boolean]],
    props.get("extraOptions").asInstanceOf[Option[Map[String, String]]]
  )

  override def write(dataFrame: DataFrame): Unit = {
    val writer = dataFrame.write

    val currentTimestamp = System.currentTimeMillis()
    fileOutputProperties.format match {
      case Some(format) => writer.format(format)
      case None         => writer.format("orc")
    }
    fileOutputProperties.quoteAll match {
      case Some(quoteAll) => writer.option("quoteAll", quoteAll)
      case None           =>
    }
    fileOutputProperties.delimiter match {
      case Some(delimiter) => writer.option("delimiter", delimiter)
      case None            =>
    }

    fileOutputProperties.partitionBy match {
      case Some(partitionBy) => writer.partitionBy(partitionBy: _*)
      case None              =>
    }

    fileOutputProperties.saveMode match {
      case Some(saveMode) => writer.mode(saveMode)
      case None           =>
    }

    fileOutputProperties.header match {
      case Some(header) => writer.option("header", header)
      case None         =>
    }

    fileOutputProperties.extraOptions match {
      case Some(options) => writer.options(options)
      case None          =>
    }

    // Handle path
    val path: Option[String] =
      (fileOutputProperties.path, fileOutputProperties.createUniquePath) match {
        case (Some(path), Some(true)) =>
          Option(path + "/" + currentTimestamp.toString + "/")
        case (Some(path), _) => Option(path)
        case _               => None
      }
    path match {
      case Some(filePath) => writer.option("path", filePath)
      case None           =>
    }

    writer.save()
  }

  def protectFromEmptyOutput(ss: SparkSession,
                             protectFromEmptyOutput: Option[Boolean],
                             format: Option[String],
                             path: String,
                             tableName: String): Unit = {
    fileOutputProperties.protectFromEmptyOutput match {
      case Some(true) => {
        val dfFromFile = fileOutputProperties.format match {
          case Some(format) => {
            ss.read.format(format.toLowerCase).load(path)
          }
          case _ => ss.read.parquet(path)
        }
        if (dfFromFile.head(1).isEmpty) {
          throw GeneralException(s"Aborting  data files are empty!")
        }
      }
      case _ =>
    }
  }

  case class FileOutputProperties(path: Option[String],
                                  createUniquePath: Option[Boolean],
                                  saveMode: Option[String],
                                  partitionBy: Option[Seq[String]],
                                  format: Option[String],
                                  delimiter: Option[String],
                                  header: Option[String],
                                  quoteAll: Option[String],
                                  protectFromEmptyOutput: Option[Boolean],
                                  extraOptions: Option[Map[String, String]])

}
