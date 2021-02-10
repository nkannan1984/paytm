package com.paytm.exam.transformations.Impl

import java.text.SimpleDateFormat
import java.util.Date

import com.paytm.exam.readers.file.FileInput
import com.paytm.exam.transformations.Transform
import com.paytm.exam.writers.file.CSVOutputWriter
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.slf4j.LoggerFactory

import scala.util.Success
import scala.util.Try

object WeatherTransformation extends Transform {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def transformation(context: SparkContext,
                              sqlContext: SQLContext): Unit = {

    //Reading Properties
    val weatherInputPath = context.getConf.get("spark.weatherInputPath")
    val stationListInputPath = context.getConf.get("spark.stationListInputPath")
    val countryListInputPath = context.getConf.get("spark.countryListInputPath")

    //Creating DataFrame
    val weatherData = FileInput(
      weatherInputPath,
      Option(Map("delimiter" -> ",", "header" -> "true")),
      Option("com.databricks.spark.csv")
    ).read(sqlContext)

    val stationList = FileInput(
      stationListInputPath,
      Option(Map("delimiter" -> ",", "header" -> "true")),
      Option("com.databricks.spark.csv")
    ).read(sqlContext)

    val countryList = FileInput(
      countryListInputPath,
      Option(Map("delimiter" -> ",", "header" -> "true")),
      Option("com.databricks.spark.csv")
    ).read(sqlContext)

    val stationWithCountry = stationList.join(
      countryList,
      upper(stationList.col("COUNTRY_ABBR")) === upper(
        countryList.col("COUNTRY_ABBR")
      )
    )

    val stationCountryWeather = stationWithCountry.join(
      weatherData,
      upper(stationWithCountry.col("STN_NO")) === upper(
        weatherData.col("STN---")
      )
    )

    stationCountryWeather.createOrReplaceTempView("stageTable")

    val countryWithHottestTemp = sqlContext.sql(
      "select COUNTRY_FULL , AVG(TEMP)  from stageTable where TEMP <> '9999.9' GROUP BY COUNTRY_FULL ORDER BY AVG(TEMP) DESC limit 1 "
    )

    countryWithHottestTemp.show(1)
    val countryWithConsecutiveTornadoes = sqlContext.sql(
      " Select COUNTRY_FULL , max(seq) from ( SELECT COUNTRY_FULL , Indicator , MIN(YEARMODA) , MAX(YEARMODA), COUNT(*) as seq from ( select COUNTRY_FULL, substring (t.FRSHTT, 6,1) as Indicator,t.YEARMODA, (t.YEARMODA - ( row_number() over (partition by COUNTRY_FULL , substring (FRSHTT, 6,1) order by YEARMODA ) ) ) as grp from stageTable t ) temp group by COUNTRY_FULL , Indicator , grp ) temp2 where Indicator = '1' group by COUNTRY_FULL order by max(seq) desc   "
    )

    countryWithConsecutiveTornadoes.show(1)

    val countryWithsecondAverageWindSpeed = sqlContext.sql(
      "select COUNTRY_FULL from ( select COUNTRY_FULL , AVG(WDSP) as averageWindSpeed from stageTable where WDSP <> '999.9' GROUP BY COUNTRY_FULL ORDER BY AVG(WDSP) desc  limit 2 ) Order by averageWindSpeed asc "
    )

    countryWithsecondAverageWindSpeed.show(1)
  }

}
