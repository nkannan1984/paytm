package com.paytm.exam.readers.jdbc

import com.paytm.exam.readers.Reader
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

case class JDBCInput(connectionUrl: String,
                     user: String,
                     password: String,
                     table: String,
                     options: Option[Map[String, String]])
    extends Reader {
  def read(SQLContext: SQLContext): DataFrame = {
    val url = connectionUrl
    val baseDBOptions = Map(
      "url" -> url,
      "user" -> user,
      "password" -> password,
      "dbTable" -> table
    )

    val DBOptions = baseDBOptions ++ options.getOrElse(Map())

    val dbTable = SQLContext.read.format("jdbc").options(DBOptions)
    dbTable.load()
  }
}
