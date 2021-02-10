package com.paytm.exam.readers.file

import org.apache.commons.io.FilenameUtils

object FileType extends Enumeration {
  type TableType = Value
  val parquet, orc, avro, csv, jsonl, json = Value

  def getFileType(path: String): TableType = {
    val extension = FilenameUtils.getExtension(path)
    if (isValidFileType(extension)) FileType.withName(extension) else orc
  }

  def isValidFileType(s: String): Boolean = values.exists(_.toString == s)
}
